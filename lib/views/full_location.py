def full_location(cityCsvDF, countryCSVDF, addressCsvDF):
    """
    This is needed to combine the city, country and full address into one df

    """

    join_expr6 = cityCsvDF.country_id == countryCSVDF.country_id
    combine_city_and_country = cityCsvDF.join(countryCSVDF, join_expr6, "left").drop(
        "country_id",
        cityCsvDF.last_update,
        cityCsvDF.country_id,
        countryCSVDF.last_update,
        countryCSVDF.country_id,
    )
    combine_city_and_country.show()

    join_expr7 = combine_city_and_country.city_id == addressCsvDF.city_id

    combine_address_and_place = combine_city_and_country.join(
        addressCsvDF, join_expr7, "inner"
    ).drop(addressCsvDF.city_id, "last_update", combine_city_and_country.city_id)

    return combine_address_and_place
