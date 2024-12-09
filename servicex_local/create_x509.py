from pathlib import Path
import subprocess
import click
import os
import tempfile


@click.command()
@click.option(
    "--proxy-image",
    default="sslhep/x509-secrets:develop",
    help="Docker image for proxy creation",
)
@click.option(
    "--globus-dir", default="~/.globus", help="Directory containing Globus files"
)
def main(proxy_image, globus_dir):
    # Create a proxy certificate

    # Find globus files.
    globus_dir_path = Path(os.path.expanduser(globus_dir))
    if not globus_dir_path.exists():
        raise FileNotFoundError(
            f"Directory {globus_dir_path} not found. Need the Globus "
            "files to create a proxy certificate."
        )
        return 1

    def create_docker_script(temp_dir):
        script_content = """#!/bin/bash
cp /globus/usercert.pem /tmp/usercert.pem
cp /globus/userkey.pem /tmp/userkey.pem
chmod 444 /tmp/usercert.pem
chmod 400 /tmp/userkey.pem
voms-proxy-init -voms atlas -cert /tmp/usercert.pem -key /tmp/userkey.pem -out /tmp/x509up
"""
        script_path = Path(temp_dir) / "run_voms_proxy.sh"
        with open(script_path, "w") as script_file:
            script_file.write(script_content)
        os.chmod(script_path, 0o755)
        return script_path

    # Create the docker script
    temp_dir = tempfile.gettempdir()
    script_path = create_docker_script(temp_dir)

    # Now run the docker image that will run the script.
    subprocess.run(
        [
            "docker",
            "run",
            "-it",
            "--mount",
            f"type=bind,source={globus_dir_path},readonly,target=/globus",
            "-v",
            f"{temp_dir}:/tmp",
            "--rm",
            proxy_image,
            "/tmp/run_voms_proxy.sh",
        ]
    )

    # Make sure it went well
    if not Path(f"{temp_dir}/x509up").exists():
        raise Exception("Failed to create the proxy certificate")


if __name__ == "__main__":
    main()
