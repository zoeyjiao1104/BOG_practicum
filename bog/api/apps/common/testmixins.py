"""
testmixins.py

References:
- https://docs.djangoproject.com/en/4.1/topics/testing/tools/#django.test.TransactionTestCase.databases
"""

from rest_framework import status


class BulkCreateTestMixin():
    """
    A mixin for `APITestCase` instances.
    Provides tests for bulk creation methods.
    """

    def _test_bulk_create(self, num_objects: int):
        """
        A helper function to test a single bulk
        insert of data in which every record is
        unique and any pre-existing records are
        retrieved. Raises an assertion error if 
        the expected response status code and 
        created object count are not observed. 

        Parameters:
            num_objects (int): The number of
                objects to insert.

        Returns:
            None
        """
        # Prepare data
        data = self._compose_data(num_objects)

        # Make request
        response = self.client.post(self.url, data, format='json')

        # Assert expected results
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.model.objects.count(), num_objects)

    
    def _test_bulk_create_already_exists(self, num_objects: int):
        """
        A helper function to test a series of bulk
        inserts of data in which some records already
        exist in the database.

        NOTE: "Bulk create" always returns a 
        "201 - Created" status upon completion 
        when integrity errors are ignored.

        Parameters:
            num_objects (int): The number of
                objects to insert.

        Returns:
            None
        """
        # Prepare data
        data = self._compose_data(num_objects)

        # Make request
        response = self.client.post(self.url, data, format='json')

        # Assert expected results from first load
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.model.objects.count(), num_objects)

        # Repeat submission of batch
        response = self.client.post(self.url, data, format='json')

        # Assert expected results from second load
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.model.objects.count(), num_objects)


    def _test_bulk_create_with_dupes(self, num_objects: int):
        """
        A helper function to confirm that a
        bulk insert of data still succeeds when there
        are duplicate records within the same
        posted data.

        Parameters:
            num_objects (int): The number of objects
                to insert.

        Returns:
            None
        """
        # Prepare duplicate data
        batch = self._compose_data(num_objects)
        data = [*batch, *batch, *batch]

        # Make request
        response = self.client.post(self.url, data, format='json')

        # Assert expected results
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.model.objects.count(), len(batch))


    def test_bulk_creates_100(self):
        self._test_bulk_create(num_objects=100)


    def test_bulk_creates_10000(self):
        self._test_bulk_create(num_objects=10000)


    def test_bulk_create_already_exists(self):
        self._test_bulk_create_already_exists(num_objects=100)


    def test_bulk_create_with_dupes(self):
        self._test_bulk_create_with_dupes(100)


class BulkGetOrCreateTestMixin():
    """
    A mixin for `APITestCase` instances.
    Provides tests for bulk get-or-create methods.
    """

    def _test_bulk_get_or_create(self, num_objects: int):
        """
        A helper function to test a single bulk
        insert of data in which every record is
        unique and any pre-existing records are
        retrieved. Raises an assertion error if 
        the expected response status code and 
        created object count are not observed. 

        Parameters:
            num_objects (int): The number of
                objects to insert.

        Returns:
            None
        """
        # Prepare data
        data = self._compose_data(num_objects)

        # Make request
        response = self.client.post(self.url, data, format='json')

        # Assert expected results
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.model.objects.count(), num_objects)

    
    def _test_bulk_get_or_create_already_exists(
        self, 
        num_objects: int):
        """
        A helper function to test a series of bulk
        inserts of data in which some records already
        exist in the database.

        Parameters:
            num_objects (int): The number of
                objects to insert.

        Returns:
            None
        """
        # Prepare data
        data = self._compose_data(num_objects)

        # Make request
        response = self.client.post(self.url, data, format='json')

        # Assert expected results from first load
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.model.objects.count(), num_objects)

        # Repeat submission of batch
        response = self.client.post(self.url, data, format='json')

        # Assert expected results from second load
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.model.objects.count(), num_objects)


    def _test_bulk_get_or_create_with_dupes(
        self,
        num_objects: int):
        """
        A helper function to confirm that a
        bulk insert of data still succeeds when there
        are duplicate records within the same
        posted data.

        Parameters:
            num_objects (int): The number of
                objects to insert.

        Returns:
            None
        """
        # Prepare duplicate data
        batch = self._compose_data(num_objects)
        data = [*batch, *batch, *batch]

        # Make request
        response = self.client.post(self.url, data, format='json')

        # Assert expected results
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.model.objects.count(), len(batch))


    def test_bulk_get_or_create_100(self):
        self._test_bulk_get_or_create(num_objects=100)


    def test_bulk_get_or_create_10000(self):
        self._test_bulk_get_or_create(num_objects=10000)


    def test_bulk_get_or_create_already_exists(self):
        self._test_bulk_get_or_create_already_exists(num_objects=100)


    def test_bulk_get_or_create_with_dupes(self):
        self._test_bulk_get_or_create_with_dupes(100)
