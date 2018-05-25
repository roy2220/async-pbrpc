from distutils.core import setup
import re
import typing


def get_version() -> str:
    with open("async_pbrpc/__init__.py", "r") as f:
        matches = re.search(r"__version__\s*=\s*\"(\d+\.\d+\.\d+)\"", f.read())
        assert matches is not None
        return matches[1]


def parse_requirements() -> typing.Dict[str, typing.Any]:
    install_requires = []
    dependency_links = []

    with open("requirements.txt", "r") as f:
        for line in f.readlines():
            line = line.strip()

            if line == "" or line.startswith("#"):
                continue

            if line.startswith("git+"):
                matches = re.match(r".*/([^/]+).git@v(\d+\.\d+\.\d+)$", line)
                assert matches is not None
                install_require = "{}=={}".format(matches[1], matches[2])
                install_requires.append(install_require)
                dependency_link = "{}#egg={}-{}".format(matches[0], matches[1], matches[2])
                dependency_links.append(dependency_link)
            else:
                install_require = line
                install_requires.append(install_require)

    return {
        "install_requires": install_requires,
        "dependency_links": dependency_links,
    }


setup(
    name="async-pbrpc",
    version=get_version(),
    description="Lightweight RPC framework based on AsyncIO and Protocol Buffers",
    packages=["async_pbrpc"],
    py_modules=["async_pbrpc_pb2"],
    scripts=["bin/protoc-gen-pbrpc"],
    data_files=[("include", ["include/async_pbrpc.proto"])],
    python_requires=">=3.6.0",
    **parse_requirements(),
)
