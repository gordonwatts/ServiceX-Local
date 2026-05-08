def pytest_addoption(parser):
    parser.addoption("--wsl2", action="store_true", help="run WSL2 tests")
    parser.addoption("--docker", action="store_true", help="run Docker tests")
    parser.addoption("--singularity", action="store_true", help="run Singularity tests")
