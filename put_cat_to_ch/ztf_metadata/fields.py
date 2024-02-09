from ztffields import Fields


def get_rcid_centers():
    geometry = Fields.get_field_geometry(level="quadrant")
    centroid_df = geometry.centroid.get_coordinates()
    return centroid_df.rename(columns={"x": "ra", "y": "dec"})
