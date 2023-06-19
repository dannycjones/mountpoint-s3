# Mountpoint - GitHub Actions

Mountpoint uses GitHub Actions for continuous integration and automation of tasks in the repository.

## What needs to be configured?

For integration tests and benchmarking,
one or more AWS Accounts need to be configured with the required resources.

This includes:

- An IAM Role that can be assumed by GitHub OIDC
- A bucket that can be accessed by the IAM Role
- A bucket that cannot be access by the IAM Role
- Self-hosted runners

To configure the CI with the resources, populate a repository variable named `S3_TARGETS` with JSON as below:

```json
{
    "version": 1,
    "testTargets": [
        {
            "region": "us-east-1",
            "role": "arn:aws:iam::111122223333:role/MountpointGitHubIAMRole-USE1",
            "bucket": "DOC-EXAMPLE-BUCKET",
            "forbiddenBucket": "DOC-EXAMPLE-BUCKET1"
        },
        {
            "region": "us-east-2",
            "role": "arn:aws:iam::111122223333:role/MountpointGitHubIAMRole-USE2",
            "bucket": "DOC-EXAMPLE-BUCKET2",
            "forbiddenBucket": "DOC-EXAMPLE-BUCKET3"
        }
    ]
}
```

The integration test workflow will deserialize the variable contents and use it to construct a test matrix.

Additionally, configure the following environment variables for benchmarking:

- `S3_BUCKET_NAME`
- `S3_REGION`

The bucket can be the same as the buckets used for integration testing.
By default, both the integration tests and benchmarks will run under their own prefix in S3.

For ARM-based and benchmarking workflows, self-hosted runners are required.
If they are not available, the workflows will wait until the timeout is reached.
