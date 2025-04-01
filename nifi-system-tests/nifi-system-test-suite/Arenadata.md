### Build ADB docker image
```bash
env/arenadata/build_docker_images.sh
```

### Start Arenadata integration tests
```bash
mvn clean install -P arenadata
```