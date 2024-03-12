#!/usr/bin/env python
# coding: utf-8

# # Offense :
#
#  - Pure shooter/Stretcher : focus mainly on 3pts with a good percentage, not great in other aspects
#
#  - Slasher : high %FGA in close distance 0-10, good amount of %FT per attack (Ftr - Number of FT Attempts Per FG Attempt)
#  Could try to plug the drives  statistics
#  - Versatile : standard deviation of all shootings area lower than most
#
#  - Paint Threat : high %FGA in close distance 0-3, good %orb,
#
#  - Creator : %AST,
#
# # Defense :
#
# - Paint Protector : quite high in %DRB , blocks
#
# -

#
# # Offense:
#
# **Pure Shooter/Stretcher**:
#
# - Three-Point Shooting Percentage (3P%): Indicates the player's accuracy from beyond the arc. ✅
# - Three-Point Attempt Rate (3PAr): Measures the proportion of field goal attempts taken from three-point range. ✅
# - Effective Field Goal Percentage (eFG%): Reflects a player's shooting efficiency, giving extra weight to three-pointers made. ✅
# - Usage Rate (USG%): Provides insight into how involved the player is in the team's offensive possessions. ✅
#
# **Slasher**:
#
# - Field Goal Percentage (FG%): Measures overall shooting accuracy. ✅ *but not sure*
# - Percentage of Field Goals Attempted within 0-10 Feet (FGA 0-10 ft): Shows the player's tendency to attack the basket. ✅
# - Free Throw Rate (FTR): Indicates how frequently the player gets to the free-throw line relative to their field goal attempts. ✅
# - Drives Per Game: Quantifies how often the player attempts to penetrate into the paint off the dribble. ❌
#
# **Versatile Scorer**:
#
# - Standard Deviation of Field Goal Percentages: Reflects consistency in shooting across different zones.✅
# - Points Per Possession (PPP) in Isolation and Pick & Roll: Highlights scoring efficiency in isolation and pick-and-roll situations. *not sure if pertinent*
# - Assists per Game (AST): Demonstrates the player's ability to create scoring opportunities for teammates.✅ *not sure pertinent*
#
# **Paint Threat**:
#
# - Percentage of Field Goals Attempted within 0-3 Feet (FGA 0-3 ft): Indicates proficiency in scoring close to the basket. ✅
# - Offensive Rebound Percentage (ORB%): Reflects the player's ability to grab offensive rebounds. ✅
# - Effective Field Goal Percentage (eFG%): Reflects a player's shooting efficiency, favorising players with a high 2FG%. ✅
#
# **Creator/Facilitator**:
#
# - Assist Percentage (AST%): Reflects the percentage of teammate field goals a player assists on while on the court. ✅
# - Usage Rate (USG%): Indicates the player's involvement in initiating and orchestrating offensive plays. ✅
# - Assist-to-Turnover Ratio (AST/TO): Highlights the player's ability to facilitate scoring while minimizing turnovers. ❌ *to be created*
#
# # Defense:
#
# **Paint Protector/Rim Defender**:
#
# - Defensive Rebound Percentage (DRB%): Reflects the player's ability to secure defensive rebounds. ✅
# - Blocks Percentage (BLK%): Measures the player's shot-blocking prowess. ✅
# - Opponent Field Goal Percentage at the Rim: Indicates the effectiveness of the player in deterring shots near the basket. ❌
# - Defensive Box Plus/Minus (DBPM): Provides an estimate of the player's defensive contribution per 100 possessions. *maybe WS would be more convenient?* ✅
#
# **Perimeter Lockdown Defender**:
#
# - Steals percentage (STL%): Quantifies the player's ability to disrupt passing lanes and create turnovers. ✅
# - Deflections Per Game: Indicates the player's activity in deflecting passes and disrupting offensive plays.
# - Defensive Box Plus/Minus (DBPM): Provides an estimate of the player's defensive contribution per 100 possessions. *maybe WS would be more convenient?* ✅
# - Opponent Field Goal Percentage: Reflects the shooting efficiency of opponents when guarded by the player. ❌
#
# **Switchable Defender**:
#
# - Defensive Versatility Index: Reflects the player's ability to guard multiple positions effectively.
# - Defensive Win Shares (DWS): Quantifies the player's defensive contribution to team wins.
# - Pick & Roll Defensive Efficiency: Measures effectiveness in defending pick-and-roll plays.
# - Defensive Rebound Percentage (DRB%): Shows the player's ability to secure rebounds on the defensive end.
#
# **Rebounding Specialist**:
#
# - Total Rebound Percentage (TRB%): Reflects the player's ability to secure both offensive and defensive rebounds. ✅
# - Offensive Rebound Percentage (DRB%): Indicates the player's effectiveness in grabbing offensive rebounds. ✅
# - Box Outs Per Game: Demonstrates the player's effort in boxing out opponents to secure rebounds. ❌
#
# **Defensive Anchor**:
#
# - Defensive Box Plus/Minus (DBPM): Provides an estimate of the player's defensive contribution per 100 possessions. *maybe WS would be more convenient?* ✅
# - Overall defensive statistics : combine (blocks, steals, deflection, box out)
# - Opponent Field Goal Percentage Differential: Compares the shooting efficiency of opponents when the player is on and off the court. ❌

# In[ ]:


# In[2]:


def offensive_profile(player_stats, player_stats_ranked) -> str:
    """
    Determine the offensive category of a player based on their statistics.

    Args:
    - player_stats (pd.Series): A pandas Series containing the offensive statistics of a player.

    Returns:
    - str: The offensive category of the player.
    """

    ## Creator/Facilitator
    if (
        (player_stats["AST%"] >= 25.0)
        and (player_stats["USG%"] >= 20.0)
        and (player_stats["AST/TOV"] >= 1.5)
    ):
        return "Creator/Facilitator"

    ## Pure Shooter/Stretcher:
    elif (
        (player_stats["3P%"] > 0.37)
        and (player_stats["3PAr"] > 0.5)
        and (player_stats_ranked["eFG% ranked"] <= 4)
        and (player_stats["USG%"] <= 20.0)
    ):
        return "Pure Shooter/Stretcher"

    # Paint Threat
    elif (
        (player_stats["%FGA 0-3"] > 0.5)
        and (player_stats_ranked["ORB% ranked"] <= 3)
        and (player_stats_ranked["eFG% ranked"] <= 3)
    ):
        return "Paint Threat"

    ## Slasher
    elif (
        (player_stats["%FGA 0-3"]) > 0.2
        and (player_stats["%FGA 3-10"] + player_stats["%FGA 10-16"]) > 0.2
        and (player_stats_ranked["FTr ranked"] <= 4)
        and (player_stats["%FGA 3P"] < 20.0)
    ):
        return "Slasher"

    ## Versatile Scorer
    elif (
        (player_stats_ranked["std_areas_FGA ranked"] >= 5)
        and (player_stats["USG%"] >= 20.0)
        and (player_stats_ranked["FG ranked"] <= 5)
    ):
        return "Versatile Scorer"

    else:
        return "No significant offensive role"


def defensive_profile(player_stats, player_stats_ranked) -> str:
    """
    Determine the defensive category of a player based on their statistics.

    Args:
    - player_stats (pd.Series): A pandas Series containing the statistics of a player.

    Returns:
    - str: The defensive category of the player.
    """

    ## Paint Protector/Rim Defender
    if (
        player_stats["DRB%"] >= 25.0
        and player_stats_ranked["BLK% ranked"] <= 3
        and player_stats_ranked["DBPM"] <= 3
    ):
        return "Paint Protector/Rim Defender"

    ## Perimeter Lockdown Defender
    elif player_stats["STL%"] >= 1.5 and player_stats_ranked["DBPM ranked"] <= 4:
        return "Perimeter Lockdown Defender"

    ## Switchable Defender
    elif (
        player_stats["DRB%"] >= 20.0
        and player_stats_ranked["BLK% ranked"] <= 6
        and player_stats_ranked["DBPM ranked"] <= 4
        and player_stats_ranked["DWS ranked"] <= 4
    ):
        return "Switchable Defender"

    ## Rebounding Specialist
    elif (
        player_stats["TRB%"] >= 10.0
        and player_stats_ranked["DRB% ranked"] <= 2
        and player_stats["DRB%"] >= 20.0
    ):
        return "Rebounding Specialist"

    ## Defensive Anchor
    # elif player_stats['DBPM'] >= 2 & overall_defensive_statistics >= 80 & player_stats['Opponent FG% Differential'] >= 2:
    # return 'Defensive Anchor'

    else:
        return "No significant defensive role"
