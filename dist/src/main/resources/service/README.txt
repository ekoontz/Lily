Service wrapper
===============

This directory contains the configuration files and launch scripts for
the Tanuki Java Service Wrapper, which is included within Lily.

For basic usage, simply execute the lily-service or lily-service.bat
scripts.

When you copy or link the scripts elsewhere or when you install
the Windows service, you will need to edit the wrapper.conf file, and
adjust the value of the following property to the location where
Lily is installed:

set.default.LILY_HOME=

To install as a Windows service, execute the install-lily-service.bat script.

In case of problems, see the log file in LILY_HOME/logs/lily-wrapper.log.
If that does not help, try setting wrapper.debug=TRUE in wrapper.conf.

   +-----------------------------------------------------------+
   | If you find the service wrapper useful, please visit the  |
   | Tanuki Software website for more information about        |
   | licensing and sponsoring.                                 |
   |                                                           |
   | http://wrapper.tanukisoftware.org/                        |
   +-----------------------------------------------------------+
