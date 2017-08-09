# README #

SCRIMP is designed to monitor a queue for idle jobs then acquire cloud instances to fulfill them. SCRIMP can be operated as a service where multiple gateways, or queues can be subscribed to a single provisioning instance. SCRIMP will then monitor these queues and acquire instances to join their work pools.

### Simulation plugin ###

SCRIMP contains a simulation plugin that can be used to explore how provisioning alogrithms perform with AWS instances. We use a history of real instance provisioning attempts to construct a distribution of times for an instance to be acquired. Bid prices can also be examined as the simulator checks real AWS prices for each instance type to determine when bids would result in instance terminations. Tests can be run at any time within the previous 90 days (to use AWS's spot price histories).

### Who do I talk to? ###

Any questions, please feel free to email me at rchard@anl.gov