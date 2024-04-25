from pyspark.sql import *
from lib.logger import Log4J
from lib.schema import (
    actor,
    address,
    category,
    city,
    country,
    customer,
    film_actor,
    film_category,
    film,
    inventory,
    language,
    payment,
    rental,
    staff,
    store,
)
from lib.utils.tables import load_df_fail_fast, load_df_permissive
from lib.views.full_location import full_location
from lib.views.dimmovie import create_dimmovies
from lib.views.dimstore import create_dimstore
from lib.views.dimcustomer import create_dimcustomer
from lib.views.dimdate import create_dimdate
from lib.views.factsales import create_factsales


if __name__ == "__main__":
    # conf = get_spark_app_config()
    spark = SparkSession.builder.master("local[3]").appName("DVDRental").getOrCreate()

    logger = Log4J(spark)

    # -------------------Schemas----------------------------
    actorSchemaStruct = actor.ActorSchema.get_schema()
    addressSchemaStruct = address.AddressSchema.get_schema()
    categorySchemaStruct = category.CategorySchema.get_schema()
    citySchemaStruct = city.CitySchema.get_schema()
    countrySchemaStruct = country.CountrySchema.get_schema()
    customerSchemaStruct = customer.CustomerSchema.get_schema()
    filmActorSchemaStruct = film_actor.FilmActorSchema.get_schema()
    filmCategorySchemaStruct = film_category.FilmCategorySchema.get_schema()
    filmSchemaStruct = film.FilmSchema.get_schema()
    inventorySchemaStruct = inventory.InventorySchema.get_schema()
    languageSchemaStruct = language.LanguageSchema.get_schema()
    paymentSchemaStruct = payment.PaymentSchema.get_schema()
    rentalSchemaStruct = rental.RentalSchema.get_schema()
    staffSchemaStruct = staff.StaffSchema.get_schema()
    storeSchemaStruct = store.StoreSchema.get_schema()

    # --------------------Dataframes-----------------------------
    actorCsvDF = load_df_fail_fast(spark, actorSchemaStruct, "data/actor.csv")
    addressCsvDF = load_df_permissive(spark, addressSchemaStruct, "data/address.csv")
    categoryCsvDF = load_df_fail_fast(spark, categorySchemaStruct, "data/category.csv")
    cityCsvDF = load_df_fail_fast(spark, citySchemaStruct, "data/city.csv")
    countryCSVDF = load_df_fail_fast(spark, countrySchemaStruct, "data/country.csv")
    customerCSVDF = load_df_permissive(spark, customerSchemaStruct, "data/customer.csv")
    filmActorCSVDF = load_df_fail_fast(
        spark, filmActorSchemaStruct, "data/film_actor.csv"
    )
    filmCategoryCSVDF = load_df_fail_fast(
        spark, filmCategorySchemaStruct, "data/film_category.csv"
    )
    filmCSVDF = load_df_fail_fast(spark, filmSchemaStruct, "data/film.csv")
    inventoryCSVDF = load_df_fail_fast(
        spark, inventorySchemaStruct, "data/inventory.csv"
    )
    languageCSVDF = load_df_fail_fast(spark, languageSchemaStruct, "data/language.csv")
    paymentCSVDF = load_df_fail_fast(spark, paymentSchemaStruct, "data/payment.csv")
    rentalCSVDF = load_df_fail_fast(spark, rentalSchemaStruct, "data/rental.csv")
    staffCSVDF = load_df_permissive(spark, staffSchemaStruct, "data/staff.csv")
    storeCSVDF = load_df_fail_fast(spark, storeSchemaStruct, "data/store.csv")

    # -------------------Views----------------------------------
    dimmovies = create_dimmovies(filmCSVDF, languageCSVDF)
    dimmovies.show()

    # Simple way to combine the city and country into a dataframe
    combined_full_location = full_location(cityCsvDF, countryCSVDF, addressCsvDF)

    dimstore = create_dimstore(staffCSVDF, storeCSVDF, combined_full_location)
    dimstore.show()

    dimcustomer = create_dimcustomer(combined_full_location, customerCSVDF)
    dimcustomer.show()

    dimdate = create_dimdate(paymentCSVDF)
    dimdate.show()

    factsales = create_factsales(rentalCSVDF, inventoryCSVDF, paymentCSVDF)
    factsales.show()
