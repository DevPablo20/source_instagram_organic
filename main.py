import sys

from airbyte_cdk.entrypoint import launch

from source_instagram_organic import SourceInstagramOrganic

if __name__ == "__main__":
    launch(SourceInstagramOrganic(), sys.argv[1:])
