from cumulus_granule_to_cnm import __version__

# Expected tests
# input full normal CMA object, scan and get list of granules, convert said granules to CNM and save to S3 as event
# input simple list of granules, convert said granules to CNM and save to S3 as event
# empty list -> Error out
# 1-n bad granules -> continue with good ones, error out on bad one


def test_version():
    assert __version__ == '0.1.0'
