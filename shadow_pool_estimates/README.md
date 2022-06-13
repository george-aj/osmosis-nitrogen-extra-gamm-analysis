--- Methodology ---

Build a shadow pool for each join and exit. Modify this pool the following way:

Joins: Calc the excess gamm received and subtract it from the total shares of the shadow pool
Clean Exits: No adjustments just a subtraction of their gamm shares in from the shadow pools total shares and same for tokens and calc the difference in token amount
Dirty Exits: Adjust their gamm to what it should have been and remove from the shadow pools total shares and the same for tokens.

I included all the files you should need to be able to run it, even if you don't have those protos.

--- Exit Types ---

Clean Exits are ones that either have no join detected between the start and halth height or the pool had no joins period. If a user has no join detected that means they didn't get any extra gamm.

Dirty Exits are ones that a join was detected within the start and halt heights.

--- Log ---

shadow_pool_changes.csv contains all changes made to the pool as each tx was processed.
