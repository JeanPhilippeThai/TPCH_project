import generate_fake_data
import get_newest_data_oltp

def main():
    ans = get_newest_data_oltp.get_newest_data_oltp()
    print(ans)
    
if __name__ == "__main__":
    main()