#!/bin/bash
tmp_dir=$(mktemp -d -t ci-XXXXXXXXXX)
cd $tmp_dir

# source /etc/profile.d/startup-atlas.sh
setupATLAS
asetup AnalysisBase,25.2.12,here
source /mnt/c/Users/gordo/Code/iris-hep/ServiceX-Local/tests/genfiles_raw/query2_xaod/transform_single_file.sh root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/DAOD_PHYSLITE.37622528._000013.pool.root.1 /mnt/c/Users/gordo/AppData/Local/Temp/pytest-of-gordo/pytest-36/test_wsl2_science0/output/DAOD_PHYSLITE.37622528._000013.pool.root.1  # noqa
