build:
    cmake --build build

setup:
    cmake -GNinja -Bbuild -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=Debug

image:
    docker build -t noisepage .

container:
    podman run --privileged -it -d --name "dev-noisepage" -v /path/to/noisepage/:/repo/ localhost/noisepage:latest /bin/bash

pre-commit:
    pre-commit run -a
