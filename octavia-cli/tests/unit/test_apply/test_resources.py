#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import pytest
from airbyte_api_client import ApiException
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
        return {"exotic_attribute": "foo", "configuration": {"foo": "bar"}}

    def test_init_no_remote_resource(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(resources.BaseResource, "_get_state_from_file", mocker.Mock(return_value=None))
        mocker.patch.object(resources.BaseResource, "_get_remote_resource", mocker.Mock(return_value=False))
        mocker.patch.object(resources, "compute_checksum")
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.workspace_id == "workspace_id"
        assert resource.local_configuration == local_configuration
        assert resource.configuration_path == "bar.yaml"
        assert resource.api_instance == resource.api.return_value
        resource.api.assert_called_with(mock_api_client)
        assert resource.state == resource._get_state_from_file.return_value
        assert resource.remote_resource == resource._get_remote_resource.return_value
        assert resource.was_created == resource._get_remote_resource.return_value
        assert resource.local_file_changed is True
        assert resource.resource_id is None

    def test_init_with_remote_resource_not_changed(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(
            resources.BaseResource, "_get_state_from_file", mocker.Mock(return_value=mocker.Mock(configuration_checksum="my_checksum"))
        )
        mocker.patch.object(resources.BaseResource, "_get_remote_resource", mocker.Mock(return_value={"resource_id": "my_resource_id"}))

        mocker.patch.object(resources, "compute_checksum", mocker.Mock(return_value="my_checksum"))
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.was_created is True
        assert resource.local_file_changed is False
        assert resource.resource_id == "my_resource_id"

    def test_init_with_remote_resource_changed(self, mocker, patch_base_class, mock_api_client, local_configuration):
        mocker.patch.object(
            resources.BaseResource,
            "_get_state_from_file",
            mocker.Mock(return_value=mocker.Mock(configuration_checksum="my_state_checksum")),
        )
        mocker.patch.object(resources.BaseResource, "_get_remote_resource", mocker.Mock(return_value={"resource_id": "my_resource_id"}))
        mocker.patch.object(resources, "compute_checksum", mocker.Mock(return_value="my_new_checksum"))
        resource = resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")
        assert resource.was_created is True
        assert resource.local_file_changed is True
        assert resource.resource_id == "my_resource_id"

    @pytest.fixture
    def resource(self, patch_base_class, mock_api_client, local_configuration):
        return resources.BaseResource(mock_api_client, "workspace_id", local_configuration, "bar.yaml")

    def test_get_attr(self, resource, local_configuration):
        assert resource.exotic_attribute == local_configuration["exotic_attribute"]
        with pytest.raises(AttributeError):
            resource.wrong_attribute

    def test_search(self, resource):
        search_results = resource._search()
        assert search_results == resource._search_fn.return_value
        resource._search_fn.assert_called_with(resource.api_instance, resource.search_payload)

    @pytest.mark.parametrize(
        "search_results,expected_error,expected_output",
        [
            ([], None, None),
            (["foo"], None, "foo"),
            (["foo", "bar"], resources.DuplicateRessourceError, None),
        ],
    )
    def test_get_remote_resource(self, resource, mocker, search_results, expected_error, expected_output):
        mock_search_results = mocker.Mock(return_value=search_results)
        mocker.patch.object(resource, "_search", mocker.Mock(return_value=mocker.Mock(get=mock_search_results)))
        if expected_error is None:
            remote_resource = resource._get_remote_resource()
            assert remote_resource == expected_output
        else:
            with pytest.raises(expected_error):
                remote_resource = resource._get_remote_resource()
        resource._search.return_value.get.assert_called_with("universal_resources", [])

    @pytest.mark.parametrize(
        "state_path_is_file",
        [True, False],
    )
    def test_get_state_from_file(self, mocker, resource, state_path_is_file):
        mocker.patch.object(resources, "os")
        mock_expected_state_path = mocker.Mock(is_file=mocker.Mock(return_value=state_path_is_file))
        mocker.patch.object(resources, "Path", mocker.Mock(return_value=mock_expected_state_path))
        mocker.patch.object(resources, "ResourceState")
        state = resource._get_state_from_file()
        resources.os.path.dirname.assert_called_with(resource.configuration_path)
        resources.os.path.join.assert_called_with(resources.os.path.dirname.return_value, "state.yaml")
        resources.Path.assert_called_with(resources.os.path.join.return_value)
        if state_path_is_file:
            resources.ResourceState.from_file.assert_called_with(mock_expected_state_path)
            assert state == resources.ResourceState.from_file.return_value
        else:
            assert state is None

    @pytest.mark.parametrize(
        "was_created",
        [True, False],
    )
    def test_get_diff_with_remote_resource(self, mocker, resource, was_created):
        mocker.patch.object(resource, "was_created", was_created)
        mock_remote_resource = mocker.Mock()
        mocker.patch.object(resource, "remote_resource", mock_remote_resource)
        mocker.patch.object(resources, "compute_diff")
        if was_created:
            diff = resource.get_diff_with_remote_resource()
            resources.compute_diff.assert_called_with(resource.remote_resource.connection_configuration, resource.configuration)
            assert diff == resources.compute_diff.return_value.pretty.return_value
        else:
            with pytest.raises(resources.NonExistingRessourceError):
                resource.get_diff_with_remote_resource()

    def test_create_or_update(self, mocker, resource):
        expected_results = {resource.resource_id_field: "resource_id"}
        operation_fn = mocker.Mock(return_value=expected_results)
        mocker.patch.object(resources, "ResourceState")
        payload = "foo"
        result, state = resource._create_or_update(operation_fn, payload)
        assert result == expected_results
        assert state == resources.ResourceState.create.return_value
        resources.ResourceState.create.assert_called_with(resource.configuration_path, "resource_id")

    @pytest.mark.parametrize(
        "response_status,expected_error",
        [(404, ApiException), (422, resources.InvalidConfigurationError)],
    )
    def test_create_or_update_error(self, mocker, resource, response_status, expected_error):
        operation_fn = mocker.Mock(side_effect=ApiException(status=response_status))
        mocker.patch.object(resources, "ResourceState")
        with pytest.raises(expected_error):
            resource._create_or_update(operation_fn, "foo")

    def test_create(self, mocker, resource):
        mocker.patch.object(resource, "_create_or_update")
        assert resource.create() == resource._create_or_update.return_value
        resource._create_or_update.assert_called_with(resource._create_fn, resource.create_payload)

    def test_update(self, mocker, resource):
        mocker.patch.object(resource, "_create_or_update")
        assert resource.update() == resource._create_or_update.return_value
        resource._create_or_update.assert_called_with(resource._update_fn, resource.update_payload)
