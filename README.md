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

This text is a **DRAFT**

To use this, example code is as follows:

```python
    codegen = LocalXAOD()
    science_runner = ScienceWSL2("atlas_al9", "22.2.107")
    adaptor = SXLocalAdaptor(codegen, science_runner)

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
                        "tests/test.root",  # noqa: E501
                    ]
                ),
                "Query": jet_info_per_event,
            }
        ]
    }
    files = deliver(spec, servicex_name="servicex-uc-af", sx_adaptor=adaptor)
    assert files is not None, "No files returned from deliver! Internal error"
```

## Acknowledgments

This `docker` versions of this code are thanks to @ketan96-m's work on [this Service MR](https://github.com/ssl-hep/ServiceX/pull/828).
