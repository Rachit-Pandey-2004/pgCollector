import asyncio
from asyncpg import exceptions, create_pool
from socket import gaierror
from datetime import datetime
from configparser import ConfigParser
from os import getcwd

class RGDB:
    _task=None
    def __init__(self, filename="%s/src/db/config.ini"%getcwd(), section="postgresql") -> None:
        parser = ConfigParser()
        parser.read(filename)
        self.db_config = {}
        if parser.has_section(section):
            parms = parser.items(section)
            for items in parms:
                self.db_config[items[0]] = items[1]
        else: 
            raise Exception("Section {0} not found in the {1} file".format(section, filename))
        if RGDB._task is None:
            RGDB._task = asyncio.create_task(self._cleaner_loop()) # Need to setup
        
    async def __aenter__(self):
        connection_successful = await self.__Stablish_Connection()
        if connection_successful:
            if self.db_config['generate_table'] != "False":
                table_created = await self.__generate_tables() # need to setup
                if table_created:
                    return self
                raise Exception("failed - table generation sequence to execute")
            return self
            
        raise Exception("Failed! no connection was stablished with the database")

    async def __aexit__(self, exc_type, exc_value, traceback):
        if hasattr(self, 'pool') and self.pool:
            await self.pool.close()

    async def __Stablish_Connection(self) -> bool:
        print("generating table")
        try:
            self.pool = await create_pool(
                host=self.db_config["hostname"],
                port=self.db_config["port_number"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"],
                min_size=10,
                max_size=20,
                command_timeout=10,
                max_inactive_connection_lifetime=5
            )
            print("new connection to db was stablished successfully")
            return True

        except exceptions.InvalidAuthorizationSpecificationError:
            print("{0} dosen't exists".format(self.db_config["user"]))
        except exceptions.InvalidPasswordError:
            print("wrong password for the user {1}".format(self.db_config["user"]))
        except exceptions.InvalidCatalogNameError:
            print("database dosen't exists")
        except gaierror:
            print("Invalid host")
        except OSError:
            print("Connection failed might be due to port number")
        except ValueError:
            print("wrong format for port number")
        except Exception as error:
            print("Error during stablishing the connection\n{0}".format(error))
        return False
    
    async def __generate_tables(self):
        try:
            async with self.pool.acquire() as conn:
                print("pool connection was stablished")
                await conn.execute(
                    """--sql
                    CREATE TABLE IF NOT EXISTS raid_coords(
                        id SERIAL PRIMARY KEY,
                        gym_name VARCHAR(255) NOT NULL,
                        ex_raid_eligible BOOLEAN NOT NULL DEFAULT FALSE,
                        sponsor BOOLEAN NOT NULL DEFAULT FALSE,
                        location GEOGRAPHY(POINT, 4326) NOT NULL,  -- PostGIS spatial data type
                        raid_spawn TIMESTAMP NOT NULL,
                        raid_start TIMESTAMP NOT NULL,
                        raid_end TIMESTAMP NOT NULL,
                        pokemon_id INT NOT NULL DEFAULT 0,
                        level INT NOT NULL CHECK (level BETWEEN 1 AND 5),
                        cp INT NOT NULL DEFAULT -1,
                        team INT NOT NULL CHECK (team BETWEEN 0 AND 3),  -- 0 = Neutral, 1 = Mystic, 2 = Valor, 3 = Instinct
                        move1 INT NOT NULL DEFAULT -1,
                        move2 INT NOT NULL DEFAULT -1,
                        is_exclusive BOOLEAN NOT NULL DEFAULT FALSE,
                        form INT NOT NULL DEFAULT 0,
                        gender INT NOT NULL CHECK (gender BETWEEN 0 AND 2)  -- 0 = Unknown, 1 = Male, 2 = Female
                    );
                    
                    -- Indexes for performance optimization
                    CREATE INDEX idx_gym_location ON gym_raids USING GIST (location); -- Spatial index for fast geo queries
                    CREATE INDEX idx_raid_time ON gym_raids (raid_start, raid_end); -- Index for filtering by raid times
                    CREATE INDEX idx_pokemon_level ON gym_raids (pokemon_id, level); -- Optimize Pokémon-based searches
                    CREATE INDEX idx_team ON gym_raids (team); -- Optimize team-based lookups
                    """
                )
                print("Successfully table was created...")
            return True
        except Exception as e:
            print(f"Failed to create tables: {e}")           
        return False
    async def __cleaning_sequence(self):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM raid_coords WHERE raid_end < NOW();
                    """
                )
                print("cleaning sequence executes sucessfully")
        except Exception as e:
            print("FAILURE IN CLEANING SEQUENCE \n %s"%e)

    async def _cleaner_loop(self):
        print("cleaner loaded successfully ...")
        await asyncio.sleep(10)
        while True:
            await asyncio.sleep(60)
            await self.__cleaning_sequence()
    
    async def insert_mass_data(self, data: list) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany("""
                    INSERT INTO gym_raids (
                        gym_name, ex_raid_eligible, sponsor, location, 
                        raid_spawn, raid_start, raid_end, pokemon_id, 
                        level, cp, team, move1, move2, is_exclusive, form, gender
                    ) VALUES (
                        $1, $2, $3, ST_SetSRID(ST_MakePoint($5, $4), 4326),
                        to_timestamp($6), to_timestamp($7), to_timestamp($8),
                        $9, $10, $11, $12, $13, $14, $15, $16
                    )
                    ON CONFLICT (gym_name, ROUND(CAST(ST_X(location::geometry) AS NUMERIC), 5),
                                ROUND(CAST(ST_Y(location::geometry) AS NUMERIC), 5), raid_start)
                    DO NOTHING
                """, data)
                return True
        except Exception as e:
            print(f"Failed to insert raid data: {e}")
            return False