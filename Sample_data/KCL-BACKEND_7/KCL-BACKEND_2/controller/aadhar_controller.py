# def aadhar_details():
#     name = "Test Sample"
#     dob = "20/12/1999"
#     gender = "male",
#     address = \
#         {
#             "addresslineOne": "456, block 10000, building 200000",
#             "addresslineTwo": "xyz society,abc area",
#             "City": "City 123",
#             "State": "State 123",
#             "pincode": "2345617",
#             "mobilenumber" : "98765421901"
#         }
#
#     return name, dob, gender, address


import random


def aadhar_details():
    # --- Define 4 test cases (2 valid age, 2 invalid age) ---

    test_cases = [
        # ---------------------------------------------
        # 1. Valid age (between 16–28) + valid pincode
        # ---------------------------------------------
        {
            "name": "Valid User 1",
            "dob": "15/08/2004",  # age ~21
            "gender": "male",
            "address": {
                "addresslineOne": "123 Valid Street",
                "addresslineTwo": "Green Park",
                "City": "Port Blair",
                "State": "Andaman",
                "pincode": "744301",
                "mobilenumber": "9876543210"
            }
        },

        # ---------------------------------------------
        # 2. Valid age (between 16–28) + valid pincode
        # ---------------------------------------------
        {
            "name": "Valid User 2",
            "dob": "10/01/1998",  # age ~26
            "gender": "female",
            "address": {
                "addresslineOne": "456 Hill Road",
                "addresslineTwo": "Blue Colony",
                "City": "Port Blair",
                "State": "Andaman",
                "pincode": "744302",
                "mobilenumber": "9123456780"
            }
        },

        # ---------------------------------------------
        # 3. Invalid age (<16 or >28) + invalid pincode
        # ---------------------------------------------
        {
            "name": "Invalid Age User 1",
            "dob": "05/03/2012",  # age ~13
            "gender": "male",
            "address": {
                "addresslineOne": "999 Invalid Lane",
                "addresslineTwo": "Old City",
                "City": "Chandigarh",
                "State": "Punjab",
                "pincode": "160036",
                "mobilenumber": "9012345678"
            }
        },

        # ---------------------------------------------
        # 4. Invalid age (<16 or >28) + invalid pincode
        # ---------------------------------------------
        {
            "name": "Invalid Age User 2",
            "dob": "22/05/1985",  # age ~40
            "gender": "female",
            "address": {
                "addresslineOne": "78 Remote Area",
                "addresslineTwo": "South Zone",
                "City": "Coimbatore",
                "State": "Tamil Nadu",
                "pincode": "642133",
                "mobilenumber": "9090909090"
            }
        }
    ]

    # Pick a random case each call
    selected = random.choice(test_cases)

    return (
        selected["name"],
        selected["dob"],
        selected["gender"],
        selected["address"]
    )
