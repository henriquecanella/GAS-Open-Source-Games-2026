from src import filter, gh_client

def main():
    print("Hello from open-games-survey!")
    #filter.filter_csv()
    gh_client.get_gh_data()


if __name__ == "__main__":
    main()
