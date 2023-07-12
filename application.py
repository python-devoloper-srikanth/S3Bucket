from io import BytesIO
import os

from logic.s3_dict import S3Dict


class UseS3Dict:
    def manipulate_s3dict_bucket(self):

        """
        Check S3 dict class functioning.

        Args:
            
        Returns:
            
        """

        # Create an instance of S3Dict
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        bucket_name = os.getenv("BUCKET_NAME")
        region_name = os.getenv("REGION")
        s3_dict = S3Dict(
            bucket_name=bucket_name,
            region_name=region_name,
            access_key=access_key,
            secret_key=secret_key,
        )

        # Put a new object in the bucket
        s3_dict["Organic Cotton Industry"] = BytesIO(
            b"Organic cotton is grown without the use of synthetic fertilizers, pesticides, and genetically modified organisms (GMOs). This industry promotes sustainable farming practices and uses natural methods for pest and weed control. Organic cotton is commonly used in the textile industry to produce chemical-free fabrics."
        )
        s3_dict["Linen Industry"] = BytesIO(
            b" Linen is a natural fiber made from the flax plant. The production of linen involves minimal chemical use, as flax plants require fewer pesticides and fertilizers compared to other crops. Linen is known for its durability, breathability, and hypoallergenic properties."
        )
        s3_dict["Hemp Industry"] = BytesIO(
            b"Hemp fibers are derived from the Cannabis sativa plant and are used in textile production. Hemp cultivation typically requires fewer chemicals compared to other crops, as it is naturally resistant to pests and diseases. Hemp textiles are known for their strength, breathability, and sustainability."
        )
        s3_dict["Tencel or Lyocell Industry"] = BytesIO(
            b"Tencel or Lyocell is a fiber made from the cellulose found in wood pulp, usually sourced from sustainably managed forests. The production process of Tencel involves a closed-loop system, where solvents used in the manufacturing process are recycled and reused. Tencel fabrics are known for their softness, moisture-wicking properties, and minimal environmental impact."
        )
        s3_dict["Bamboo Industry"] = BytesIO(
            b"Bamboo fibers are derived from the bamboo plant, which is known for its fast growth and low pesticide requirements. Bamboo textiles are produced using mechanical or chemical processes. In mechanical processing, the fibers are mechanically crushed and combed, requiring minimal chemical use. Look for bamboo textiles that are certified as organic or made through a closed-loop system to minimize chemical inputs."
        )

        # Get an object from the bucket
        obj = s3_dict["Linen Industry"]
        print(obj.read())  # Output: b'This is the content of object 1'

        # Check if an object exists
        print("Tencel or Lyocell Industry" in s3_dict)  # Output: True

        # Remove an object from the bucket
        del s3_dict["Tencel or Lyocell Industry"]

        # Check if an object exists (after deletion)
        print("Tencel or Lyocell Industry" in s3_dict)  # Output: False

        # Generate all keys in the bucket
        for key in s3_dict.keys():
            print(key)

        # Generate key-value pairs in the bucket
        for key, value in s3_dict.items():
            print(key, value.read())


if __name__ == "__main__":
    obj = UseS3Dict()
    obj.manipulate_s3dict_bucket()
