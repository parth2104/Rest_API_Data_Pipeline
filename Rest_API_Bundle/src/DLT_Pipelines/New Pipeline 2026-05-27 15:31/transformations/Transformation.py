from pyspark.sql.types import *
from pyspark.sql.functions import *
import dlt

# Get c

# Bronze volume path
volume_path = "/Volumes/project_dev/bronze/products"

# Products schema
product_schema = ArrayType(
    StructType(
        [
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("discountPercentage", DoubleType(), True),
            StructField("rating", DoubleType(), True),
            StructField("stock", IntegerType(), True),
            StructField("brand", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("weight", DoubleType(), True),
            StructField("warrantyInformation", StringType(), True),
            StructField("shippingInformation", StringType(), True),
            StructField("availabilityStatus", StringType(), True),
            StructField("returnPolicy", StringType(), True),
            StructField("minimumOrderQuantity", IntegerType(), True),
        ]
    )
)


# Silver layer table
@dlt.table(name="products_data")
def product_data():

    # Read raw JSON files from Bronze layer
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
    )

    # Parse products JSON string
    parsed_df = df.select(
        from_json(col("products").cast("string"), product_schema).alias(
            "parsed_products"
        )
    )

    # Normalize array into rows
    exploded_df = parsed_df.select(explode(col("parsed_products")).alias("product"))

    # Flatten nested structure
    final_df = exploded_df.select(
        col("product.id").alias("id"),
        col("product.title").alias("title"),
        col("product.description").alias("description"),
        col("product.category").alias("category"),
        col("product.price").alias("price"),
        col("product.discountPercentage").alias("discount_percentage"),
        col("product.rating").alias("rating"),
        col("product.stock").alias("stock"),
        col("product.brand").alias("brand"),
        col("product.sku").alias("sku"),
        col("product.weight").alias("weight"),
        col("product.warrantyInformation").alias("warranty_information"),
        col("product.shippingInformation").alias("shipping_information"),
        col("product.availabilityStatus").alias("availability_status"),
        col("product.returnPolicy").alias("return_policy"),
        col("product.minimumOrderQuantity").alias("minimum_order_quantity"),
    )
    final_df = final_df.withColumn(
        "brand",
        when(
            col("brand").isNull() | (trim(col("brand")) == ""), lit("Generic")
        ).otherwise(col("brand")),
    )

    return final_df
