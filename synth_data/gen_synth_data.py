from values import CURRENCY_VARIANTS, STATUS_VARIANTS, PAYMENT_METHOD_VARIANTS
import csv
import os
import random
import uuid
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
load_dotenv()

#transaction_id
#transaction_ts
#user_id
#amount
#currency
#status
#product_id
#payment_method

#Possible errors:
# 1. diffrent column name
# 2. user_id = NULL or empty
# 3. transaction ts = NULL
# 4 currency out of standart list
# 5. amount <= 0 // string // comma
# 6. transaction_id X2
# status -> Sta T us

OUTPUT_DIR = os.getenv("INCOMING_DIR")
ROWS_PER_FILE = 500

PRODUCT_IDS = ["P" + str(i).zfill(4) for i in range(1, 115)]

def main():
    filename = "payments_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + ".csv"
    tmp_path = os.path.join(OUTPUT_DIR, "." + filename + ".tmp")
    final_path = os.path.join(OUTPUT_DIR, filename)

    col1= random.choice(["transaction_id", "id", "tranc_id"])
    col2= random.choice(["transaction_ts", "ttime", "tdate"])
    col3= random.choice(["user_id", "id", "user"])
    col4= random.choice(["amount"])
    col5= random.choice(["currency", "cur"])
    col6= random.choice(["status", "state"])
    col7= random.choice(["product_id", "id", "product"])
    col8= random.choice(["payment_method", "pay_m", "paym"])

    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            col1,
            col2,
            col3,
            col4,
            col5,
            col6,
            col7,
            col8,
        ])
        writer.writeheader()

        for _ in range(ROWS_PER_FILE):
            transaction_id = str(uuid.uuid4())
            base_dt = datetime.now() + timedelta(days=random.randint(-2, 0))
            transaction_ts = base_dt.strftime("%Y-%m-%dT%H:%M:%S")

            user_state = random.choice(["ok", "none", "empty"])
            if user_state == "ok":
                user_id = random.randint(999, 10000)
            elif user_state == "none":
                user_id = None
            else:
                user_id = ""

            amount_value = round(random.uniform(-25, 3000), 2)
            amount_case = random.choice(["number", "string", "comma"])

            if amount_case == "string":
                amount = " " + str(amount_value) + " "
            elif amount_case == "comma":
                amount = str(amount_value).replace(".", ",")
            else:
                amount = amount_value

            currency = random.choice(CURRENCY_VARIANTS)
            status = random.choice(STATUS_VARIANTS)
            product_id = random.choice(PRODUCT_IDS)
            payment_method = random.choice(PAYMENT_METHOD_VARIANTS)

            writer.writerow({
                col1: transaction_id,
                col2: transaction_ts,
                col3: user_id,
                col4: amount,
                col5: currency,
                col6: status,
                col7: product_id,
                col8: payment_method
            })

    os.replace(tmp_path, final_path)
    print("Geerated file: ", filename)

if __name__ == "__main__":
    while True:
        main()
        time.sleep(300)
