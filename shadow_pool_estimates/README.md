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

Rows where was_exit_clean = 0 the pool_adjustment_amount_1 and pool_adjustment_amount_2 columns are how much extra someone got as a result of a join that gave extra gamm. 

Rows where was_exit_clean = 1 (Clean Exit) the pool_adjustment_amount_1/pool_adjustment_amount_2 are how much more they should have gotten. To get the amount of the difference subtract the sender_original_amount_1/sender_original_amount_2 columns.







