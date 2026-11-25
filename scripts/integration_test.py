from common import new_event, publish, verify_ingestion, wait_pipeline


def main():
    wait_pipeline()
    event = new_event()
    publish(event)
    verify_ingestion(event)
    print("Integration pipeline test passed.")


if __name__ == "__main__":
    main()
