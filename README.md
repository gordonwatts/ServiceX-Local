# ServiceX-Local

A drop in to enable local running of the ServiceX codegen and science images. This is
mostly geared toward running for debugging and testing.

## Installation

Install this as a library in your virtual environment with:

* `pip install servicex-local` for all the Docker based codegen and science container access.
* `pip install servicex-local[xAOD]` to get a local xAOD code generator (and the required dependencies) along with the docker based code.

Some science images requires a x509 certificate to run. You'll need to get the `x509up` into your `/tmp` area. If you don't have the tools installed locally, do the following:

1. Make sure in your `~/.globus` directory (on whatever OS you are no) contains the `usercert.pem` and `userkey.pem` files
1. Use the `voms_proxy_init` to initialize against the `atlas` voms.

The science image code will pick up the location of the 509 cert.

## Usage

### Certificates

This will do its best to track `x509` certs. If a file called `x509up` is located in your temp directory (including on windows), that will be copied into the `docker` image or other places to be used.

### Example Code

This text is a **DRAFT**

To use this, example code is as follows:

```python
    codegen = LocalXAODCodegen()
    science_runner = WSL2ScienceImage("atlas_al9", "25.2.12")
    adaptor = SXLocalAdaptor(
        codegen, science_runner, "atlasr22", "http://localhost:5001"
    )

    from servicex.configuration import Configuration, Endpoint

    Configuration.register_endpoint(
        Endpoint(
            name="test-backend",
            adapter=adaptor,
            minio=MinioLocalAdaptor.for_transform,
            # TODO: Endpoint is still required, even though not used.
            endpoint="bogus",
        )
    )

    logging.basicConfig(level=logging.DEBUG)

    # The simple query, take straight from the example in the documentation.
    query = q.FuncADL_ATLASr22()  # type: ignore
    jets_per_event = query.Select(lambda e: e.Jets("AnalysisJets"))
    jet_info_per_event = jets_per_event.Select(
        lambda jets: {
            "pt": jets.Select(lambda j: j.pt()),
            "eta": jets.Select(lambda j: j.eta()),
        }
    )

    spec = {
        "Sample": [
            {
                "Name": "func_adl_xAOD_simple",
                "Dataset": dataset.FileList(
                    [
                        "root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/"
                        "DAOD_PHYSLITE.37622528._000013.pool.root.1"
                    ]
                ),
                "Query": jet_info_per_event,
                "IgnoreLocalCache": True,
            }
        ]
    }
    files = deliver(
        spec,
        servicex_name="test-backend",
    )
```

### Running tests

If you are on a machine with `wsl2` and or `docker` you can run the complete set of tests with flags:

```bash
pytest --wsl2 --docker
```

## Acknowledgments

This `docker` versions of this code are thanks to @ketan96-m's work on [this Service MR](https://github.com/ssl-hep/ServiceX/pull/828).
