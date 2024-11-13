# kafka-tester
## v0.3
kafka traffic generator for telemetry testing  
Will send subscriptions as listed in subscriptions.json  
Details of kafka broker, and list of devices in kafka-tester.yaml  
To run *./kafka-tester.app*
Requires files 
* kafka-tester.yaml
* subscriptions.json

More devices are added in *kafka-tester.yaml* add to the list: *devices*  
Subscriptions are the list of subscription message produced. The index in each subscription is the scaling/multiplier of each subscription message.

## Release
- v0.1 04/11/2024 Initial release numbering
- v0.2 07/11/2024
    - Removed seperate *list_devices.json* file
    - Behaviour change of index listing
- v0.3 13/11/2024
    - Added behaviour for subscription with no indexes