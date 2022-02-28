#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import pytest
from octavia_cli.apply import resources


class TestBaseResource:
    @pytest.fixture
    def patch_base_class(self, mocker):
        # Mock abstract methods to enable instantiating abstract class
        mocker.patch.object(resources.BaseResource, "__abstractmethods__", set())
        mocker.patch.object(resources.BaseResource, "create_function_name", "create_resource")
        mocker.patch.object(resources.BaseResource, "resource_id_field", "resource_id")
        mocker.patch.object(resources.BaseResource, "search_function_name", "search_resource")
        mocker.patch.object(resources.BaseResource, "update_function_name", "update_resource")
        mocker.patch.object(resources.BaseResource, "resource_type", "universal_resource")
        mocker.patch.object(resources.BaseResource, "api")

    @pytest.fixture
    def local_configuration(self):
        return {"exotic_attribute": "foo"}

    def test_init_no_remote_resource(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(resources.BaseResource, "get_state", mocker.Mock(return_value=None))
        mocker.patch.object(resources.BaseResource, "get_remote_resource", mocker.Mock(return_value=False))
        mocker.patch.object(resources, "compute_checksum")
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.workspace_id == "workspace_id"
        assert resource.local_configuration == local_configuration
        assert resource.configuration_path == "bar.yaml"
        assert resource.api_instance == resource.api.return_value
        resource.api.assert_called_with(mock_api_client)
        assert resource.state == resource.get_state.return_value
        assert resource.remote_resource == resource.get_remote_resource.return_value
        assert resource.was_created == resource.get_remote_resource.return_value
        assert resource.local_file_changed is True

    def test_init_with_remote_resource_not_changed(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(
            resources.BaseResource, "get_state", mocker.Mock(return_value=mocker.Mock(configuration_checksum="my_checksum"))
        )
        mocker.patch.object(resources.BaseResource, "get_remote_resource", mocker.Mock(return_value=True))
        mocker.patch.object(resources, "compute_checksum", mocker.Mock(return_value="my_checksum"))
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.was_created is True
        assert resource.local_file_changed is False

    def test_init_with_remote_resource_changed(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(
            resources.BaseResource, "get_state", mocker.Mock(return_value=mocker.Mock(configuration_checksum="my_state_checksum"))
        )
        mocker.patch.object(resources.BaseResource, "get_remote_resource", mocker.Mock(return_value=True))
        mocker.patch.object(resources, "compute_checksum", mocker.Mock(return_value="my_new_checksum"))
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.was_created is True
        assert resource.local_file_changed is True

    def test_get_attr(self, patch_base_class, mock_api_client, local_configuration):
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.exotic_attribute == local_configuration["exotic_attribute"]
        with pytest.raises(AttributeError):
            resource.wrong_attribute
