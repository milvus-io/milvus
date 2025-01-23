import pytest
from base.testbase import TestBase
from utils.utils import gen_unique_str


@pytest.mark.L0
class TestDatabaseOperation(TestBase):
    """
    Test cases for database operations
    """

    def test_create_database_with_default_properties(self):
        """
        Test creating a database with default properties
        """
        db_name = f"test_db_{gen_unique_str()}"
        payload = {"dbName": db_name}
        rsp = self.database_client.database_create(payload)
        assert rsp["code"] == 0

        # Verify database exists
        list_rsp = self.database_client.database_list({})
        assert rsp["code"] == 0
        assert db_name in list_rsp["data"]

    def test_create_database_with_custom_properties(self):
        """
        Test creating a database with custom properties
        """
        db_name = f"test_db_{gen_unique_str()}"
        payload = {"dbName": db_name, "properties": {"mmap.enabled": True}}
        rsp = self.database_client.database_create(payload)
        assert rsp["code"] == 0

        # Verify properties
        describe_rsp = self.database_client.database_describe({"dbName": db_name})
        assert describe_rsp["code"] == 0
        assert any(
            prop["key"] == "mmap.enabled" and prop["value"] == "true"
            for prop in describe_rsp["data"]["properties"]
        )

    def test_alter_database_properties(self):
        """
        Test altering database properties
        """
        db_name = f"test_db_{gen_unique_str()}"

        # Create database with initial properties
        create_payload = {"dbName": db_name, "properties": {"mmap.enabled": True}}
        rsp = self.database_client.database_create(create_payload)
        assert rsp["code"] == 0
        # Verify properties
        describe_rsp = self.database_client.database_describe({"dbName": db_name})
        assert describe_rsp["code"] == 0
        assert any(
            prop["key"] == "mmap.enabled" and prop["value"] == "true"
            for prop in describe_rsp["data"]["properties"]
        )

        # Alter properties
        alter_payload = {"dbName": db_name, "properties": {"mmap.enabled": False}}
        alter_rsp = self.database_client.database_alter(alter_payload)
        assert alter_rsp["code"] == 0

        # Verify altered properties
        describe_rsp = self.database_client.database_describe({"dbName": db_name})
        assert describe_rsp["code"] == 0
        assert any(
            prop["key"] == "mmap.enabled" and prop["value"] == "false"
            for prop in describe_rsp["data"]["properties"]
        )

    def test_list_databases(self):
        """
        Test listing databases
        """
        # Create test database
        db_name = f"test_db_{gen_unique_str()}"
        self.database_client.database_create({"dbName": db_name})

        # List databases
        rsp = self.database_client.database_list({})
        assert rsp["code"] == 0
        assert "default" in rsp["data"]  # Default database should always exist
        assert db_name in rsp["data"]

    def test_describe_database(self):
        """
        Test describing database
        """
        db_name = f"test_db_{gen_unique_str()}"
        properties = {"mmap.enabled": True}

        # Create database
        self.database_client.database_create(
            {"dbName": db_name, "properties": properties}
        )

        # Describe database
        rsp = self.database_client.database_describe({"dbName": db_name})
        assert rsp["code"] == 0
        assert rsp["data"]["dbName"] == db_name
        assert "dbID" in rsp["data"]
        assert len(rsp["data"]["properties"]) > 0


@pytest.mark.L0
class TestDatabaseOperationNegative(TestBase):
    """
    Negative test cases for database operations
    """

    def test_create_database_with_invalid_name(self):
        """
        Test creating database with invalid name
        """
        invalid_names = ["", " ", "test db", "test/db", "test\\db"]
        for name in invalid_names:
            rsp = self.database_client.database_create({"dbName": name})
            assert rsp["code"] != 0

    def test_create_duplicate_database(self):
        """
        Test creating database with duplicate name
        """
        db_name = f"test_db_{gen_unique_str()}"

        # Create first database
        rsp1 = self.database_client.database_create({"dbName": db_name})
        assert rsp1["code"] == 0

        # Try to create duplicate
        rsp2 = self.database_client.database_create({"dbName": db_name})
        assert rsp2["code"] != 0

    def test_describe_non_existent_database(self):
        """
        Test describing non-existent database
        """
        rsp = self.database_client.database_describe({"dbName": "non_existent_db"})
        assert rsp["code"] != 0

    def test_alter_non_existent_database(self):
        """
        Test altering non-existent database
        """
        payload = {"dbName": "non_existent_db", "properties": {"mmap.enabled": False}}
        rsp = self.database_client.database_alter(payload)
        assert rsp["code"] != 0

    def test_drop_non_existent_database(self):
        """
        Test dropping non-existent database
        """
        rsp = self.database_client.database_drop({"dbName": "non_existent_db"})
        assert rsp["code"] == 0

    def test_drop_default_database(self):
        """
        Test dropping default database (should not be allowed)
        """
        rsp = self.database_client.database_drop({"dbName": "default"})
        assert rsp["code"] != 0


@pytest.mark.L0
class TestDatabaseProperties(TestBase):
    """Test database properties operations"""

    def test_alter_database_properties(self):
        """
        target: test alter database properties
        method: create database, alter database properties
        expected: alter database properties successfully
        """
        # Create database
        client = self.database_client
        db_name = "test_alter_props"
        payload = {
            "dbName": db_name
        }
        response = client.database_create(payload)
        assert response["code"] == 0

        # Alter database properties
        properties = {"mmap.enabled": False}
        response = client.alter_database_properties(db_name, properties)
        assert response["code"] == 0

        # Drop database properties
        property_keys = ["mmap.enabled"]
        response = client.drop_database_properties(db_name, property_keys)
        assert response["code"] == 0

        # Clean up
        client.database_drop({"dbName": db_name})

    @pytest.mark.parametrize("invalid_property", [
        {"invalid_key": True},
        {"mmap.enabled": "invalid_value"}
    ])
    @pytest.mark.xfail(reason="issue: https://github.com/milvus-io/milvus/issues/39545")
    def test_alter_database_properties_with_invalid_properties(self, invalid_property):
        """
        target: test alter database properties with invalid properties
        method: create database, alter database properties with invalid properties
        expected: alter database properties failed with error
        """
        # Create database
        client = self.database_client
        db_name = "test_alter_props_invalid"
        payload = {
            "dbName": db_name
        }
        response = client.database_create(payload)
        assert response["code"] == 0

        # Alter database properties with invalid property
        response = client.alter_database_properties(db_name, invalid_property)
        assert response["code"] == 1100

        # Clean up
        client.database_drop({"dbName": db_name})

    @pytest.mark.xfail(reason="issue: https://github.com/milvus-io/milvus/issues/39545")
    def test_drop_database_properties_with_nonexistent_key(self):
        """
        target: test drop database properties with nonexistent key
        method: create database, drop database properties with nonexistent key
        expected: drop database properties failed with error
        """
        # Create database
        client = self.database_client
        db_name = "test_drop_props_nonexistent"
        payload = {
            "dbName": db_name
        }
        response = client.database_create(payload)
        assert response["code"] == 0

        # Drop database properties with nonexistent key
        property_keys = ["nonexistent.key"]
        response = client.drop_database_properties(db_name, property_keys)
        assert response["code"] == 1100

        # Clean up
        client.database_drop({"dbName": db_name})
