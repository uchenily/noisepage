build:
    cmake --build build --target noisepage -- -j 12

setup:
    cmake -GNinja -Bbuild -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_UNITY_BUILD=OFF

image:
    docker build -t noisepage .

container:
    podman run --privileged -it -d --name "dev-noisepage" -v /path/to/noisepage/:/repo/ localhost/noisepage:latest /bin/bash

pre-commit:
    pre-commit run -a

debug:
    gdb -x debug.gdb
