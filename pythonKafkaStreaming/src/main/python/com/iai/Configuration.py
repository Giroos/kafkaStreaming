import ConfigParser

class Configuration:

    Config = None

    def __init__(self):
        self.Config = ConfigParser.ConfigParser()
        self.Config.read("D:\\JavaProjects\\kafkaStreamingPython\\src\\main\\resources\\config.ini")

    def ConfigSectionMap(self, section):
        dict1 = {}
        options = self.Config.options(section)
        for option in options:
            try:
                dict1[option] = self.Config.get(section, option)
                if dict1[option] == -1:
                    print("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1

    def getProperty(self,section,property):
        return self.ConfigSectionMap(section)[property]

