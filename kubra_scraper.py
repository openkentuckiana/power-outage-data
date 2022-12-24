import mercantile
import polyline
import requests
from dotenv import load_dotenv

from base_scraper import DeltaScraper

load_dotenv()

MIN_ZOOM = 7
# They don't appear to let us zoom in beyond 15.
# They just group incidents that aren't resolvable at zoom level 14, which isn't great.
MAX_ZOOM = 14


class KubraScraper(DeltaScraper):
    base_url = "https://kubra.io/"

    record_key = "id"
    noun = "outage"

    total_downloaded = 0
    total_requests = 0

    @property
    def config_url(self):
        return f"{self.base_url}stormcenter/api/v1/stormcenters/{self.instance_id}/views/{self.view_id}/configuration/{self.deploymentId}?preview=false"

    @property
    def data_url(self):
        return f"{self.base_url}{self.data_path}/public/summary-1/data.json"

    @property
    def service_areas_url(self):
        return f"{self.base_url}{self.regions}/{self.regions_key}/serviceareas.json"

    @property
    def state_url(self):
        return (
            f"{self.base_url}stormcenter/api/v1/stormcenters/{self.instance_id}/views/{self.view_id}/currentState?preview=false"
        )

    def __init__(self, github_token):
        super().__init__(github_token)
        state = self._make_request(self.state_url).json()
        self.regions_key = list(state["datastatic"])[0]
        self.regions = state["datastatic"][self.regions_key]
        self.data_path = state["data"]["interval_generation_data"]
        self.cluster_data_path = state["data"]["cluster_interval_generation_data"]

        self.deploymentId = state["stormcenterDeploymentId"]
        config = self._make_request(self.config_url).json()
        interval_data = config["config"]["layers"]["data"]["interval_generation_data"]
        self.layer_name = [l for l in interval_data if l["type"].startswith("CLUSTER_LAYER")][0]["id"]

    def fetch_data(self):
        data = self._make_request(self.data_url).json()
        expected_outages = data["summaryFileData"]["totals"][0]["total_outages"]

        quadkeys = self._get_service_area_quadkeys()

        outages = self._fetch_data(quadkeys, set()).values()
        number_out = sum([o["numberOut"] for o in outages])

        print(f"Made {self.total_requests} requests, fetching {self.total_downloaded/1000} KB.")

        if number_out != expected_outages:
            raise Exception(f"Outages found ({number_out}) does not match expected outages ({expected_outages})")

        return list(outages)

    def display_record(self, outage):
        display = [f"  {outage['custAffected']} outage(s) added with {outage['custAffected']} customers affected"]
        return "\n".join(display)

    def _fetch_data(self, quadkeys, already_seen, zoom=MIN_ZOOM, cluster_search=False, print_prepend=""):
        outages = {}

        for q in quadkeys:
            url = self._get_quadkey_url(q)
            if url in already_seen:
                print(print_prepend, "Skipping", url)
                continue
            already_seen.add(url)
            res = self._make_request(url)

            print(print_prepend, url, "Is cluster search?:", cluster_search, "Tile:", self._get_tile_for_quadkey(q))

            # If there are no outages in the area, there won't be a file.
            if not res.ok:
                print(print_prepend, "Not found")
                continue

            for o in res.json()["file_data"]:
                print(print_prepend, o)
                if o["desc"]["cluster"]:
                    # If it's a cluster, we need to drill down (zoom in)
                    next_zoom = zoom + 1
                    if next_zoom > MAX_ZOOM:
                        print("We are at max zoom, we can't resolve incidents grouped closer than zoom level 14.")
                        outage_info = self._get_outage_info(o, url)
                        outages[outage_info["id"]] = outage_info
                    else:
                        print(print_prepend, "Zooming in. Going to:", next_zoom)
                        outages.update(
                            self._fetch_data(
                                [self._get_quadkey_for_point(o["geom"]["p"][0], next_zoom)],
                                already_seen,
                                next_zoom,
                                True,
                                print_prepend + "    ",
                            )
                        )
                else:
                    print(print_prepend, "Looking for neighbors")
                    outages.update(
                        self._fetch_data(
                            self._get_neighboring_quadkeys(q), already_seen, zoom, False, print_prepend + "    "
                        )
                    )

                    outage_info = self._get_outage_info(o, url)
                    outages[outage_info["id"]] = outage_info
        print(print_prepend, "Returning")
        return outages

    def _get_quadkey_url(self, quadkey):
        data_path = self.cluster_data_path.format(qkh=quadkey[-3:][::-1])
        return f"{self.base_url}{data_path}/public/{self.layer_name}/{quadkey}.json"

    def _get_service_area_quadkeys(self):
        """Get the quadkeys for the entire service area"""
        res = self._make_request(self.service_areas_url).json()
        areas = res.get("file_data")[0].get("geom").get("a")

        points = []
        for geom in areas:
            # Geometries are in Google's Polyline Algorithm format
            # https://developers.google.com/maps/documentation/utilities/polylinealgorithm
            points += polyline.decode(geom)

        bbox = self._get_bounding_box(points)

        return [mercantile.quadkey(t) for t in mercantile.tiles(*bbox, zooms=[MIN_ZOOM])]

    def _make_request(self, url):
        res = requests.get(url)
        self.total_downloaded += len(res.content)
        self.total_requests += 1
        return res

    @staticmethod
    def _get_bounding_box(points):
        x_coordinates, y_coordinates = zip(*points)
        return [min(y_coordinates), min(x_coordinates), max(y_coordinates), max(x_coordinates)]

    @staticmethod
    def _get_neighboring_quadkeys(quadkey):
        tile = mercantile.quadkey_to_tile(quadkey)
        return [
            mercantile.quadkey(mercantile.Tile(x=tile.x, y=tile.y - 1, z=tile.z)),  # N
            mercantile.quadkey(mercantile.Tile(x=tile.x + 1, y=tile.y, z=tile.z)),  # E
            mercantile.quadkey(mercantile.Tile(x=tile.x, y=tile.y + 1, z=tile.z)),  # S
            mercantile.quadkey(mercantile.Tile(x=tile.x - 1, y=tile.y, z=tile.z)),  # W
            mercantile.quadkey(mercantile.Tile(x=tile.x + 1, y=tile.y - 1, z=tile.z)),  # NE
            mercantile.quadkey(mercantile.Tile(x=tile.x + 1, y=tile.y + 1, z=tile.z)),  # SE
            mercantile.quadkey(mercantile.Tile(x=tile.x - 1, y=tile.y - 1, z=tile.z)),  # NW
            mercantile.quadkey(mercantile.Tile(x=tile.x - 1, y=tile.y + 1, z=tile.z)),  # SW
        ]

    @staticmethod
    def _get_outage_info(raw_outage, url):
        desc = raw_outage["desc"]
        loc = polyline.decode(raw_outage["geom"]["p"][0])

        # If it's a cluster we can't resolve, assign an ID that is <polyline point>-<start time>
        return {
            "id": f"{raw_outage['geom']['p'][0]}-{desc['start_time']}" if not desc["inc_id"] else desc["inc_id"],
            "etr": desc["etr"],
            "etrConfidence": desc["etr_confidence"],
            "cluster": desc["cluster"],
            "comments": desc["comments"],
            "cause": desc["cause"]["EN-US"] if desc["cause"] else None,
            "numberOut": desc["n_out"],
            "custAffected": desc["cust_a"]["val"],
            "crewStatus": desc["crew_status"],
            "startTime": desc["start_time"],
            "latitude": loc[0][0],
            "longitude": loc[0][1],
            "source": url,
        }

    @staticmethod
    def _get_quadkey_for_point(point, zoom):
        ll = polyline.decode(point)[0]
        return mercantile.quadkey(mercantile.tile(lng=ll[1], lat=ll[0], zoom=zoom))

    @staticmethod
    def _get_tile_for_quadkey(quadkey):
        return mercantile.quadkey_to_tile(quadkey)
