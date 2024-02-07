from ztffields import Fields


def get_rcid_centers():
    geometry = Fields.get_field_geometry(level="quadrant")
    centroid_df = geometry.centroid.get_coordinates()
    # Rename fieldid to field
    centroid_df.index = centroid_df.index.set_name("field", level=0)
    return centroid_df.rename(columns={"x": "ra", "y": "dec"})
