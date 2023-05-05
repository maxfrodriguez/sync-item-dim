from mygeotab import API as GeoTabAPI


class GeotabHelper:
    @staticmethod
    def autenticate_geotab(username: str, password: str, database: str) -> GeoTabAPI:
        client: GeoTabAPI = GeoTabAPI(username=username, password=password, database=database)
        client.authenticate()
        return client
