from analysis_tools.candidate_finder import CandidateFinder
from utils.env_utils import *
from utils.plot_utils import *

# Read API keys
fmp_api_key = read_env_variable("FMP_API_KEY")


if __name__ == "__main__":
    # Find candidate stocks
    candidate_finder = CandidateFinder(fmp_api_key)
    candidates_df = candidate_finder.find_candidates()

    # Plot results
    if candidates_df is not None and len(candidates_df) > 0:
        plot_candidates(candidates_df)

