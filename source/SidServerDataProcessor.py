__author__ = 'bnelson'

from SIDServer.Utilities import ConfigUtility
from SIDServer.Controllers import SendToSidWatchServerController

config = ConfigUtility.load('D:\SourceCode\Other\SidWatch\pySidServerDataProcessor\Config\sidwatch.cfg')

controller = SendToSidWatchServerController(config)
controller.start()


