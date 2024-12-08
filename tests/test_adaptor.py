from servicex_local.adaptor import SXAdaptor_WSL2


def test_adaptor():
    adaptor = SXAdaptor_WSL2()
    assert adaptor is not None
