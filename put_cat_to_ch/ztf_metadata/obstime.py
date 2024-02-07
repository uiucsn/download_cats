from astropy.coordinates import EarthLocation, SkyCoord
from astropy.time import Time, TimeDelta


PALOMAR = EarthLocation(lon=-116.863, lat=33.356, height=1706)  # EarthLocation.of_site('Palomar')


def exposure_start_to_mid_helio(exposure_start: Time, exptime: TimeDelta, coord: SkyCoord) -> Time:
    mid_exposure = exposure_start + 0.5 * exptime
    mid_exposure += mid_exposure.light_travel_time(coord, kind='heliocentric', location=PALOMAR)
    return mid_exposure
