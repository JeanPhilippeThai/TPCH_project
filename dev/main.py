import generate_fake_data
import get_newest_data_oltp

def main():
    # generate_fake_data.generate_fake_data()

    date_lastest_export = get_newest_data_oltp.get_date_latest_export()
    print(date_lastest_export)
    
if __name__ == "__main__":
    main()