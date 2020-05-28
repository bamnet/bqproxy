FROM docker.pkg.github.com/bamnet/bqproxy/bqproxy:latest

COPY example.yaml queries.yaml
# TODO: Replace your-project-id with a Google Cloud Project that has BiqQuery enabled.
CMD ["--project=your-project-id"]