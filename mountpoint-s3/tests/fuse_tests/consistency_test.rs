use std::fs::File;
use std::io;
use std::os::unix::prelude::FileExt;
use std::time::Duration;

use fuser::BackgroundSession;
use tempfile::TempDir;
use test_case::test_case;

use mountpoint_s3::data_cache::InMemoryDataCache;
use mountpoint_s3::fs::CacheConfig;

use crate::fuse_tests::{S3FilesystemConfig, TestClientBox, TestSessionConfig};

fn page_cache_sharing_test<F>(creator_fn: F, prefix: &str)
where
    F: FnOnce(&str, TestSessionConfig) -> (TempDir, BackgroundSession, TestClientBox),
{
    // Big enough to avoid readahead
    const OBJECT_SIZE: usize = 512 * 1024;

    let (mount_point, _session, mut test_client) = creator_fn(prefix, Default::default());

    // Create the first version of the file
    let old_contents = vec![0xaau8; OBJECT_SIZE];
    test_client.put_object("file.bin", &old_contents).unwrap();

    // Open the file before updating it remotely
    let old_file = File::open(mount_point.path().join("file.bin")).unwrap();
    let mut buf = vec![0u8; 128];
    old_file.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(buf, &old_contents[..buf.len()]);

    let new_contents = vec![0xbbu8; OBJECT_SIZE];
    test_client.put_object("file.bin", &new_contents).unwrap();

    // Open the file again, should see the new contents this time
    let new_file = File::open(mount_point.path().join("file.bin")).unwrap();
    new_file.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(buf, &new_contents[..buf.len()]);

    // The old fd should see either the old contents or fail the read
    let res = old_file.read_exact_at(&mut buf, 0);
    match res {
        Ok(()) => assert_eq!(buf, &old_contents[..buf.len()]),
        Err(e) => println!("old read failed: {e:?}"),
    }

    // Try reading a fresh page in the other order (old file first)
    let offset = OBJECT_SIZE / 2;
    let res = old_file.read_exact_at(&mut buf, offset as u64);
    match res {
        Ok(()) => assert_eq!(buf, &old_contents[offset..offset + buf.len()]),
        Err(e) => println!("old read at {offset} failed: {e:?}"),
    }
    new_file.read_exact_at(&mut buf, offset as u64).unwrap();
    assert_eq!(buf, &new_contents[offset..offset + buf.len()]);
}

#[cfg(feature = "s3_tests")]
#[test]
fn page_cache_sharing_test_s3() {
    page_cache_sharing_test(crate::fuse_tests::s3_session::new, "page_cache_sharing_test");
}

#[cfg(feature = "s3_tests")]
#[test]
fn page_cache_sharing_test_s3_with_data_cache() {
    page_cache_sharing_test(
        crate::fuse_tests::s3_session::new_with_cache(InMemoryDataCache::new(1024 * 1024)),
        "page_cache_sharing_test",
    );
}

#[test_case(""; "no prefix")]
#[test_case("page_cache_sharing_test"; "prefix")]
fn page_cache_sharing_test_mock(prefix: &str) {
    page_cache_sharing_test(crate::fuse_tests::mock_session::new, prefix);
}

#[test_case(""; "no prefix")]
#[test_case("page_cache_sharing_test"; "prefix")]
fn page_cache_sharing_test_mock_with_data_cache(prefix: &str) {
    page_cache_sharing_test(
        crate::fuse_tests::mock_session::new_with_cache(InMemoryDataCache::new(1024 * 1024)),
        prefix,
    );
}

fn avoid_stuck_cached_file_on_change_test<F>(creator_fn: F, prefix: &str)
where
    F: FnOnce(&str, TestSessionConfig) -> (TempDir, BackgroundSession, TestClientBox),
{
    const OBJECT_SIZE: usize = 512 * 1024;

    let test_session_conf = TestSessionConfig {
        filesystem_config: S3FilesystemConfig {
            cache_config: CacheConfig {
                serve_lookup_from_cache: true,
                dir_ttl: Duration::from_secs(600),
                file_ttl: Duration::from_secs(600),
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let (mount_point, _session, mut test_client) = creator_fn(prefix, test_session_conf);

    // Create the first version of the file
    let old_contents = vec![0xaau8; OBJECT_SIZE];
    test_client.put_object("file.bin", &old_contents).unwrap();

    // Open the file before updating it remotely
    let old_file = File::open(mount_point.path().join("file.bin")).unwrap();
    let mut buf = vec![0u8; 128];
    old_file.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(buf, &old_contents[..buf.len()]);

    let new_contents = vec![0xbbu8; OBJECT_SIZE];
    test_client.put_object("file.bin", &new_contents).unwrap();

    // Open the file again, but this should fail to read anything
    let new_file = File::open(mount_point.path().join("file.bin")).unwrap();
    new_file
        .read_exact_at(&mut buf, 0)
        .expect_err("should fail as object cannot be read from S3");

    // Open the file **again**, but this should now succeed as the old inode was kicked
    let new_file = File::open(mount_point.path().join("file.bin")).unwrap();
    new_file
        .read_exact_at(&mut buf, 0)
        .expect("should be OK as open resulted in a fresh S3 lookup");
    assert_eq!(buf, &new_contents[..buf.len()]);
}

#[test_case(""; "no prefix")]
#[test_case("avoid_stuck_cached_file_on_change_test"; "prefix")]
fn avoid_stuck_cached_file_on_change_test_mock(prefix: &str) {
    avoid_stuck_cached_file_on_change_test(crate::fuse_tests::mock_session::new, prefix);
}

#[cfg(feature = "s3_tests")]
#[test]
fn avoid_stuck_cached_file_on_change_test_s3() {
    avoid_stuck_cached_file_on_change_test(
        crate::fuse_tests::mock_session::new,
        "avoid_stuck_cached_file_on_change_test_s3",
    );
}

fn avoid_stuck_cached_file_on_delete_test<F>(creator_fn: F, prefix: &str)
where
    F: FnOnce(&str, TestSessionConfig) -> (TempDir, BackgroundSession, TestClientBox),
{
    const OBJECT_SIZE: usize = 512 * 1024;

    let test_session_conf = TestSessionConfig {
        filesystem_config: S3FilesystemConfig {
            cache_config: CacheConfig {
                serve_lookup_from_cache: true,
                dir_ttl: Duration::from_secs(600),
                file_ttl: Duration::from_secs(600),
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let (mount_point, _session, mut test_client) = creator_fn(prefix, test_session_conf);

    // Create the first version of the file
    let old_contents = vec![0xaau8; OBJECT_SIZE];
    test_client.put_object("file.bin", &old_contents).unwrap();

    // Open the file before removing it remotely
    let old_file = File::open(mount_point.path().join("file.bin")).unwrap();
    let mut buf = vec![0u8; 128];
    old_file.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(buf, &old_contents[..buf.len()]);

    test_client.remove_object("file.bin").unwrap();

    // Open the file again, but this should fail to read anything
    let new_file = File::open(mount_point.path().join("file.bin")).unwrap();
    let _err = new_file
        .read_exact_at(&mut buf, 0)
        .expect_err("should fail as object cannot be read from S3");

    // Open the file **again**, but this should now fail due to no entry existing!
    let new_file = File::open(mount_point.path().join("file.bin")).unwrap();
    let err = new_file
        .read_exact_at(&mut buf, 0)
        .expect_err("should be error again due to fresh S3 lookup returning no result");
    assert!(matches!(err.kind(), io::ErrorKind::NotFound));
}

#[test_case(""; "no prefix")]
#[test_case("avoid_stuck_cached_file_on_delete_test"; "prefix")]
fn avoid_stuck_cached_file_on_delete_test_mock(prefix: &str) {
    avoid_stuck_cached_file_on_delete_test(crate::fuse_tests::mock_session::new, prefix);
}

#[cfg(feature = "s3_tests")]
#[test]
fn avoid_stuck_cached_file_on_delete_test_s3() {
    avoid_stuck_cached_file_on_delete_test(
        crate::fuse_tests::mock_session::new,
        "avoid_stuck_cached_file_on_change_test_s3",
    );
}
