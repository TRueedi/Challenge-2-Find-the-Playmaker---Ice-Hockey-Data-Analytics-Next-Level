# X, Y, Team from MainPlayer and dataframe from Topic all Players
def mean_distance_to_opponents(X,Y,Team, df: pd.DataFrame):
    
    distances_to_opponents = []
    main_player_team = ""
    opponent_team = ""
    
    if Team == 'Home':
        main_player_team = "Home"
        opponent_team = "Away"
    elif Team == 'Away':
        main_player_team = "Away"
        opponent_team = "Home"   
    else:
        print("Error: The team is neither home nor away.")
        
    for i in range(1, 12):

        coords_primary = [X,Y]
        float_coords_primary = [float(coord) for coord in coords_primary]
        
        if df[f'StartPlayerTeam{i}'] == opponent_team: 
            
            if type(df[f'StartPlayerCoordinates{i}']) == str:
                coords_opponent = df[f'StartPlayerCoordinates{i}'].split(',')

            elif type(df[f'StartPlayerCoordinates{i}']) == tuple:
                coords_opponent = df[f'StartPlayerCoordinates{i}']
                
            value = math.sqrt((float_coords_primary[0] - float(coords_opponent[0])) ** 2 + (float_coords_primary[1] - float(coords_opponent[1])) ** 2)
            distances_to_opponents.append(value)

    return sum(distances_to_opponents)/len(distances_to_opponents)

#X, Y, Team from MainPlayer
def distance_to_enemy_goal(X,Y,Team):
    
    rightGoal = [26.0,0.0]
    leftGoal = [-26.0, 0.0]
    
    if Team == 'Home':
        enemygoal = rightGoal
    
    if Team == 'Away':
        enemygoal = leftGoal
        
    return math.sqrt((enemygoal[0] - X)**2 + (enemygoal[1] - Y)**2)

distance_to_enemy_goal(-3,0,'Away')