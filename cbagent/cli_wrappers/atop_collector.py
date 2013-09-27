from cbagent.collectors import Atop
from cbagent.settings import Settings


def main():
    settings = Settings()
    settings.read_cfg()

    collector = Atop(settings)
    collector.restart()
    collector.update_columns()
    if settings.update_metadata:
        collector.update_metadata()
    collector.collect()

if __name__ == '__main__':
    main()
