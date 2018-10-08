# To run locally:
# !python MovieSimilarities.py --items=input/movie_titles.csv input/ratingData.txt > sims.txt

# To run on a single EMR node:
# !python MovieSimilarities.py -r emr --items=input/movie_titles.csv input/ratingData.txt

# To run on 4 EMR nodes:
#!python MovieSimilarities.py -r emr --num-ec2-instances=4 --items=input/movie_titles.csv input/ratingData.txt

# Troubleshooting EMR jobs (subsitute your job ID):
# !python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT


from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class ExpectRating(MRJob):

    # def configure_options(self):
    #     super(ExpectRating, self).configure_options()
    #     self.add_file_option('--items', help='Path to movie_titles.csv')
    #
    # def load_movie_names(self):
    #     # Load database of movie names.
    #     self.movieNames = {}
    #
    #     with open("movie_titles.txt", encoding='ascii', errors='ignore') as f:
    #         for line in f:
    #             fields = line.split(',')
    #             self.movieNames[int(fields[0])] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_final_calculation,
                    reducer=self.reducer_final_calculation)]


    #input temp.txt

    def mapper_parse_input(self, key, line):
        # Outputs userID => (movieID, rating)
        line = re.sub('\"', ' ', line).strip()
        movieID, infostring = line.split()
        yield movieID, infostring

    def reducer_ratings_by_user(self, movieID, infoList):

        relationDict = {}
        ratingDict = {}

        for info in infoList:
            if '=' in info:
                movie, relation = info.split('=')
                relationDict[movie] = float(relation)
            if ':' in info:
                user, rating = info.split(':')
                ratingDict[user] = float(rating)

        for user_key in ratingDict:
            for movie_key in relationDict:
                output_key = (movieID, user_key)
                output_value = ratingDict[user_key] * relationDict[movie_key]
                yield output_key, output_value

    def mapper_final_calculation(self, user_movie, value):

        yield user_movie, value

    def reducer_final_calculation(self, user_movie, values):

        yield  user_movie, sum(values)





if __name__ == '__main__':
    ExpectRating.run()
