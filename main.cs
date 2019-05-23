using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MPI;

namespace MPI
{
    class Program
    {
        
        public struct Config 
        {
            MPI_File dataBlock;
            int world_rank;
            int world_size;
            char* text;
            char* textBlock;
            int textSize;
            int blockSize;
            MPI_Request request;
        }

        static void Main(string[] args)
        {
            Config config = new Config();
            MPI_Init(&argc, &argv);
            MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
            MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
            //printf("apa");
            if(config.world_rank == 0) load_file();
            MPI_Bcast(&config.blockSize, 1, MPI_INT, 0, MPI_COMM_WORLD);

            if(config.world_rank == 0)
            {
                dispurse_data();
            } 
            else 
            {
                config.textBlock = (char*)malloc ((config.blockSize)*sizeof(char));
            }
            MPI_Irecv(config.textBlock, config.blockSize, MPI_CHAR, 0, 0, 
            MPI_COMM_WORLD, &config.request);
        /* printf("%d\n", config.world_rank);
            for (int i = 0; i < config.blockSize; i++) {
                printf("buffer[%d] == %c\n", i, config.textBlock[i]);
            }
            printf("buffer = %s\n", config.textBlock); */
            MPI_Finalize();
        }

        void dispurse_data()
        {
            int i;
            for(i = 1; i < config.world_size; i++)
            {
                //printf("%c\n", config.text[i*config.blockSize]);
                MPI_Isend(&(config.text[i*config.blockSize]), config.blockSize, 
                MPI_CHAR, i, 0, MPI_COMM_WORLD, &config.request);
            }
            config.textBlock = (char*)malloc ((config.blockSize)*sizeof(char));
            for(i = 0; i < config.blockSize; i++)
            {
                config.textBlock[i] = config.text[i];
            }
        }

        void load_file()
        {
            config.text = 0;
            FILE * f;
            f = fopen ("test.txt", "rb"); //was "rb"
        
            if (f)
            {
            fseek (f, 0, SEEK_END);
            config.textSize = ftell (f);
            config.blockSize = config.textSize / config.world_size;
            fseek (f, 0, SEEK_SET);
            config.text = (char*)malloc ((config.textSize+1)*sizeof(char));
            if (config.text)
            {
                fread (config.text, sizeof(char), config.textSize, f);
            }
            fclose (f);
            }
            config.text[config.textSize] = '\0';
        /*  for (int i = 0; i < config.textSize; i++) {
                printf("buffer[%d] == %c\n", i, config.text[i]);
            }
            printf("buffer = %s\n", config.text); */
        } 
    }
}
