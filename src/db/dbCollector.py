import asyncio
from asyncpg import exceptions, create_pool
from socket import gaierror
from datetime import datetime
from configparser import ConfigParser
from os import getcwd

class PGDB:
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
        asyncio.create_task(self._cleaner_loop())
        
    async def __aenter__(self):
        connection_successful = await self.__Stablish_Connection()
        if connection_successful:
            if self.db_config['generate_table'] != "False":
                table_created = await self.__generate_tables()
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
                    CREATE TABLE IF NOT EXISTS pokemon_coords(
                    s_no SERIAL,
                    id INT NOT NULL CHECK (id>=1 AND id<=1000),
                    p_name VARCHAR(50) NOT NULL,
                    cp INT NOT NULL CHECK (cp>=-1),
                    lvl FLOAT NOT NULL CHECK (lvl>=-1.0 AND lvl<=50.0),
                    gender VARCHAR(1) NOT NULL CHECK (gender IN ('M', 'F', 'N')),
                    iv FLOAT NOT NULL CHECK (iv>=-1.0 AND iv<=100.0),
                    coordinates GEOGRAPHY(POINT, 4326) NOT NULL,
                    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    despawn TIMESTAMP NOT NULL,
                    PRIMARY KEY(s_no)
                    );
                    CREATE INDEX IF NOT EXISTS idx_coordinates ON pokemon_coords USING GIST (coordinates);
                    CREATE UNIQUE INDEX IF NOT EXISTS unique_pokemon_spawn ON pokemon_coords 
                    (id, p_name, ROUND(CAST(ST_X(coordinates::geometry) AS NUMERIC), 5),
                    ROUND(CAST(ST_Y(coordinates::geometry) AS NUMERIC), 5));
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
                    DELETE FROM pokemon_coords WHERE despawn < NOW();
                    """
                )
                print("cleaning sequence executes sucessfully")
        except Exception as e:
            print("FAILURE IN CLEANING SEQUENCE \n %s"%e)

    async def _cleaner_loop(self):
        await asyncio.sleep(400)
        while True:
            await asyncio.sleep(60)
            await self.__cleaning_sequence()

    async def insert_single_data(self, data: tuple) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    '''
                    INSERT INTO pokemon_coords(id, p_name, cp, lvl, gender, iv, coordinates, despawn)
                    VALUES ($1, $2, $3, $4, $5, $6, ST_SetSRID(ST_MakePoint($7, $8), 4326), to_timestamp($9))
                    ON CONFLICT (id, p_name, ROUND(CAST(ST_X(coordinates::geometry) AS NUMERIC), 5),
                               ROUND(CAST(ST_Y(coordinates::geometry) AS NUMERIC), 5))
                    DO NOTHING
                    ''', 
                    *data
                )
                return True
        except Exception as e:
            print("Error inserting data:", e)
            print("Data attempted:", data)
            return False

    async def insert_mass_data(self, data: list) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany("""
                    INSERT INTO pokemon_coords(id, p_name, cp, lvl, gender, iv, coordinates, despawn) 
                    VALUES ($1, $2, $3, $4, $5, $6, ST_SetSRID(ST_MakePoint($7, $8), 4326), to_timestamp($9))      
                    ON CONFLICT (id, p_name, ROUND(CAST(ST_X(coordinates::geometry) AS NUMERIC), 5),
                               ROUND(CAST(ST_Y(coordinates::geometry) AS NUMERIC), 5))
                    DO NOTHING             
                    """, data)
                return True
        except Exception as e:
            print("struck with an failure while writing logging the data is not set yet...\n%s"%e)
            return False

async def test():
    async with PGDB() as pq:
        pass
        

    