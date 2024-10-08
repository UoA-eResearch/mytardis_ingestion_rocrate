"""Test the click CLI application
"""

from pathlib import Path

from click.testing import CliRunner
from gnupg import GenKey
from mock import MagicMock, patch
from mytardis_rocrate_builder.rocrate_dataclasses.rocrate_dataclasses import Person
from pytest import mark
from requests import Response

from src.cli.main import print_lab
from src.mt_api.mt_consts import MY_TARDIS_USER, UOA

runner = CliRunner()


@mark.parametrize("archive_type", [(None), ("tar"), ("zip"), ("tar.gz")])
@mark.parametrize("duplicate_directory", [True, False])
@mark.parametrize("split_datasets", [True, False])
@patch("src.mt_api.apiconfigs.MyTardisRestAgent.mytardis_api_request")
@patch("src.mt_api.apiconfigs.MyTardisRestAgent.no_auth_request")
@patch(
    "src.ingestion_targets.print_lab_genomics.ICD11_API_agent.ICD11ApiAgent._request_token"
)
@patch(
    "src.metadata_extraction.metadata_extraction.MetadataHandlder.request_metadata_schema",
    MagicMock(return_value={}),
)
@patch(
    "src.metadata_extraction.metadata_extraction.MetadataHandlder.create_metadata_from_schema",
    MagicMock(return_value={}),
)
@patch(
    "src.mt_api.apiconfigs.MyTardisRestAgent.create_person_object",
    MagicMock(
        return_value=Person(
            name="test_person", email="", mt_identifiers=[], affiliation=UOA
        )
    ),
)
@patch("mytardis_rocrate_builder.rocrate_writer.receive_keys_for_crate", MagicMock())
@patch("mytardis_rocrate_builder.rocrate_writer.receive_keys_for_crate", MagicMock())
def test_print_lab_cli(  # pylint: disable=too-many-arguments
    mock_rest_auth_request: MagicMock,
    mock_rest_no_auth_request: MagicMock,
    mock_icd11_agent: MagicMock,
    test_print_lab_data: Path,
    test_gpg_key: GenKey,
    test_output_dir: Path,
    test_log_file: Path,
    archive_type: str | None,
    test_gpg_binary_location: str,
    duplicate_directory: bool,
    split_datasets: bool,
) -> None:
    """Test running the print Lab CLI command.
    Ensure an RO-Crate is created, files are moved and archived as needed.
    """
    # mock any API calls
    test_response = Response()
    test_response.status_code = -1
    mock_rest_auth_request.return_value = test_response
    mock_rest_no_auth_request.return_value = test_response
    mock_icd11_agent.return_value = test_response

    args = [
        "-i",
        str(test_print_lab_data),
        "-k",
        str(test_gpg_key.fingerprint),
        "--log_file",
        str(test_log_file),
        "-b True",
        "--separate_manifests",
        "--output",
        str(test_output_dir),
        "--gpg_binary",
        test_gpg_binary_location,
    ]
    if archive_type:
        args.extend(["-a", archive_type])
    if duplicate_directory:
        args.append("--duplicate_directory")
    if split_datasets:
        args.append("--split_datasets")
    MY_TARDIS_USER.pubkey_fingerprints = [test_gpg_key.fingerprint]
    response = runner.invoke(print_lab, args=args)
    # assert response.stdout == ""
    assert response.exit_code == 0

    ##assert crate has been created
    dataset_dir_name = test_output_dir
    if split_datasets:
        dataset_dir_name = test_output_dir / "Bam"
    if archive_type is None:
        # check RO-Crate has been created
        assert dataset_dir_name.is_dir()
        assert (dataset_dir_name / "data").is_dir()
        assert (dataset_dir_name / "data/ro-crate-metadata.json").is_file()

        # check files have been moved when appropriate
        assert (dataset_dir_name / "data/BAM/unaligned.sam").is_file()
        assert (
            dataset_dir_name / "data/SAM/do_not_transfer.sam"
        ).is_file() == duplicate_directory
    else:
        # check if archive has been created
        assert (dataset_dir_name.with_suffix("." + archive_type)).is_file()

    assert True
    # test if archive created correctly
