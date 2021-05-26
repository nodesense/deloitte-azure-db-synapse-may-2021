https://docs.microsoft.com/en-in/azure/synapse-analytics/security/how-to-set-up-access-control?WT.mc_id=Portal-Microsoft_Azure_Synapse

```

synapse workspace:   gksynapse2
storage gen2     :   gksynapse2storage
container        :    gksynapse2fs


Group names: 

SynapseAdministrators
SynapseContributors
SynapseComputeOperators
SynapseCredentialUsers
SQLAdmins

compose the groups with workspace, now create these grousp in your active directory, yourself will be member of this group..

gksynapse2_SynapseAdministrators
gksynapse2_SynapseContributors
gksynapse2_SynapseComputeOperators
gksynapse2_SynapseCredentialUsers
gksynapse2_SQLAdmins

---

Add yourself into member in above groups created....

```
