# bqproxy - BigQuery Proxy

[![GitHub](https://img.shields.io/github/license/bamnet/bqproxy)](https://github.com/bamnet/bqproxy/blob/master/LICENSE)

BigQuery Proxy allows you to expose the results from pre-defined BigQuery queries.

It is similiar to a BigQuery View when accessible by allUsers, but also supports parameters. Queries and paramters are configured in advanced using YAML files
then packaged into a Docker image.

For an example, check out the samples/ directory.