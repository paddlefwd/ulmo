"""
    ulmo.codwr.core
    ~~~~~~~~~~~~~~

    This module provides access to Colorado Department of Water Resources
    web services.


    .. _Colorado Department of Water Resources:

    Note that CoDWR aggregates water data from multiple agencies including
    NWIS (USGS) sites. The CoDWR and NWIS nomenclature are different so
    care should be taken to avoid inadvertently including duplicate sites
    if multiple retrieval sources are used.
"""
from future import standard_library
from suds.client import Client
# from builtins import str
# from past.builtins import basestring
# import contextlib
# import io
# import datetime
import logging

# import isodate
# import requests
# from ulmo import util
import pandas as pd

standard_library.install_aliases()

# configure logging
LOG_FORMAT = '%(message)s'
logging.basicConfig(format=LOG_FORMAT)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# Co DWR WSDL and service URLs
CODWR_WSDL_URL = "http://www.dwr.state.co.us/SMS_WebService/ColoradoWaterSMS.asmx?WSDL"
CODWR_SERVICE_URL = "http://= www.dwr.state.co.us/SMS_WebService/ColoradoWaterSMS.asmx"

# The global SUDS client - do not remove or change!
_suds_client = None

def get_water_district(div=0, wd=0, use_pandas=True, suds_cache=None):
    """ Fetches the list of water divisions/districts matching the search
    criteria available from the Co. DWR site.


    Parameters
    ----------
    div         list of divisions to retrieve, default 0 (all divisions)
    wd          list of water districts to retrieve, default 0 (all districts)
    use_pandas  boolean indicating whether to return results as a Pandas data
                frame, default True
    suds_cache  Cache for SUDS SOAP service, default none

    Note that water district list can be retrieved either by number or (partial) name.
    I.e. wd=[1,2,5] would retrieve water districts 1, 2, and 5 while wd='platte'
     would retrieve all districts with "Platte" in their name.

    Returns
    -------
    Data frame or list of matching water districts
    """
    def district_matches(tgt):
        # Check if the target water district matches the search criteria
        # need to handle both ints and strings (including partial matches)
        # Note: 'tgt' is the generated WaterDistrict object from the SOAP call.
        if type(wd[0]) is int:
            return wd == [0] or tgt.wd in wd
        else:
            return any((True for frag in wd
                        if str.lower(frag) in
                        str.lower(str(tgt.waterDistrictName))))

    # ensure div is a list of ints
    if type(div) is not list: div = [div]
    assert all(type(d) is int for d in div)

    # ensure wd is a homogeneous list of either int ot str
    if type(wd) is not list: wd = [wd]
    assert type(wd[0]) in [int,str]
    assert all(type(w) is type(wd[0]) for w in wd)


    # retrieve the list of water districts
    suds_client = _get_client(CODWR_WSDL_URL)
    wda = suds_client.service.GetWaterDistricts()

    wds = []
    for wdx in wda.WaterDistrict:
        if (div == [0] or wdx.div in div) and district_matches(wdx):
            wdd = dict(wdx)
            wds.append(wdd)

    if not wds:
        return None

    if use_pandas:
        wds = pd.DataFrame(wds)
        wds.rename(columns = {'div':'wdiv'}, inplace = True)
        print(wds.columns)

    return wds


def get_station(div=0, wd=0, abbrev=None, use_pandas=True, input_file=None):
    """Fetches a list of currently active CoDWR sites by name. If an input file is
    provided the list of site names in the file will be retrieved, otherwise the
    CoDWR web service will be queried.

    Parameters
    ==========
    input_file : ``None`` or a path to file or file object.
        If ``None`` (default), then the CoDWR webservice is queried. If a file
        containing previously retrieved CoDWR water data is provided the sites
        in the file are retrieved.

    Returns
    =======
        site_list : a python list of currently active sites
    """
    # ensure div is a list of ints
    if type(div) is not list: div = [div]
    # ensure wd is a list
    if type(wd) is not list: wd = [wd]
    # ensure name is a homogeneous list of str if it exists
    if abbrev is not None and type(abbrev) is not list:
        abbrev = [abbrev]
        assert all(type(a) is str for a in abbrev)

    stations = []
    if input_file is None:
        # use the Co DWR SOAP service
        suds_client = _get_client(CODWR_WSDL_URL)

        if abbrev is None:
            # we aren't looking for named site(s) so get all sites in div/water dist
            dists = get_water_district(div, wd, use_pandas=False)
            if dists is None:
                # no matching division/district(s)
                return None

            for d in dists:
                sites = suds_client.service.GetSMSTransmittingStations(d['div'],d['wd'])
                if sites is None:
                    return None

                sparms = suds_client.service.GetSMSTransmittingStationVariables(d['div'],d['wd'])
                if sparms is None:
                    # hmmm - we have stations but no parameters...
                    raise ValueError("Service returned no parameters for transmitting station(s).")

                params = {}
                for sp in sparms.StationVariables:
                    spd = dict(sp)
                    if spd['abbrev'] not in params:
                        params[spd['abbrev']] = []
                    params[spd['abbrev']].append(spd['variable'])

                stations = []
                for site in sites.Station:
                    sited = dict(site)
                    sited['waterDistrictName'] = d['waterDistrictName']
                    sited['parameters'] = params[sited['abbrev']]
                    stations.append(sited)

        else:
            for a in abbrev:
                ssite = suds_client.service.GetSMSTransmittingStations(Abbrev=a)
                print(ssite)

    else:
        # retrieve the list of sites in the specified file
        print("Nothing yet")

    if use_pandas is True:
        stations = pd.DataFrame(stations)

    return stations


def _get_client(wsdl_url, cache_duration=("default",)):
    """
    Open and re-use (persist) a suds.client.Client instance _suds_client throughout
    the session, to minimize WOF server impact and improve performance.  _suds_client
    is global in scope.

    Parameters
    ----------
    wsdl_url : str
        URL of a service's web service definition language (WSDL) description.
        All Colorado DWR services publish a WSDL description and this url is the
        entry point to the service.
    cache_duration: ``None`` or tuple
        suds client local cache duration for WSDL description and client object.
        Pass a cache duration tuple like ('days', 3) to set a custom duration.
        Duration may be in months, weeks, days, hours, or seconds.
        If unspecified, the suds default (1 day) will be used.
        Use ``None`` to turn off caching.

    Returns
    -------
    _suds_client : suds Client
        Newly or previously instantiated (reused) suds Client object.
    """
    global _suds_client

    print(wsdl_url)
    # Handle new or changed client request (create new client)
    if _suds_client is None or _suds_client.wsdl.url != wsdl_url:
        _suds_client = Client(wsdl_url)
        if cache_duration is None:
            _suds_client.set_options(cache=None)
        else:
            cache = _suds_client.options.cache
            # could add some error catching ...
            if cache_duration[0] == "default":
                cache.setduration(days=1)
            else:
                # noinspection PyTypeChecker
                cache.setduration(**dict([cache_duration]))

    return _suds_client

if __name__ == '__main__':
    #s = get_water_district(wd='platte')
    s = get_station(div=2,wd=11,use_pandas=True)
    print(s)
