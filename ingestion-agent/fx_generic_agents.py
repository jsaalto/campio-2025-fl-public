from langchain_openai import AzureChatOpenAI
import getpass
import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential

def invoke_llm_agent(agent_type: str, user_input: str):
    system_prompt = get_system_prompt(agent_type)
    return invoke_with_prompts(system_prompt, user_input)


def get_system_prompt(agent_type):
    if agent_type == "locations":
        return (
            "You are an assistant that analyzes the content of a business website homepage. "
            "Your task is to determine if the business has multiple physical locations. "
            "If multiple locations are found, extract the individual URL links for each location. "
            "Return your answer as a JSON array of objects, each with a single field: 'link' (the URL for the location). "
            "If there are not multiple locations, return a JSON array with a single object: set 'link' to the homepage URL. "
            "Do not include any explanation or extra text—only return the JSON array."
        )
    elif agent_type == "venue homepage":
        return (
            "You are an assistant that analyzes a markdown version of a business website page. "
            "The page may be either the homepage for a business with a single location, or a sub-page specific to one of multiple locations. "
            "Your task is to extract the following attributes for the location represented by the page: "
            "address_line_1, address_line_2 (if applicable), city, state (use a two-character code if possible, otherwise the full state name), "
            "postal_code (only the first five digits if available), name (combine the business name, a space, a dash, another space, and the location name), "
            "venue_url (the URL of the current page), homepage_url (the main homepage URL), and venue_beer_list_url (a link to the beer list specific to this venue, if available). "
            "If the page is a sub-page for a specific location, derive homepage_url from the home domain of the venue_url. "
            "If there is no sub-page and this is the homepage, venue_url and homepage_url will be the same. "
            "For venue_beer_list_url: Look for hyperlinks or buttons within the page that are labeled 'beer list', 'tap list', 'on tap', 'drinks', or similar terms. "
            "Use the associated link as the URL, converting relative links to absolute URLs using the venue_url as the base. "
            "If a link to a beer list specific to this venue starts with '#', append it to the venue_url with a '/' separator. "
            "If the current page contains a table with a listing of individual beers that includes beer styles, descriptions, and ABVs, set venue_beer_list_url to the current page's URL. "
            "If no beer list is available or the current page doesn't contain a comprehensive beer table with styles, descriptions, and ABVs, leave venue_beer_list_url as an empty string. "
            "Return all of this data as a single JSON object with these fields. "
            "If any field cannot be determined, leave it as an empty string. "
            "Do not include any explanation or extra text—only return the JSON object."
        )
    elif agent_type == "summarization":
        return "You are a helpful assistant that summarizes text. Summarize the user input."
    else:
        return "You are a helpful assistant."

def invoke_with_prompts(system_prompt: str, user_input: str):
    load_dotenv()  # Load environment variables from .env file

    openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    if not openai_endpoint:
        openai_endpoint = getpass.getpass(
            "Enter your Azure OpenAI endpoint: "
        )
    openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
    if not openai_api_key:
        openai_api_key = getpass.getpass(
            "Enter your Azure OpenAI API key: "
        )
    base_deployment_name = os.getenv("AZURE_OPENAI_BASE_DEPLOYMENT_NAME")
    if not base_deployment_name:
        base_deployment_name = getpass.getpass(
            "Enter your Azure OpenAI deployment name: "
        )    
    # Validate that we have the required credentials
    # Set up authentication to Azure OpenAI
    if openai_api_key and openai_api_key.strip():
        # Use API key authentication
        # print("API key authentication will be used.")
        # print(f"Using endpoint: {openai_endpoint}")
        # print(f"Using deployment: {base_deployment_name}")
        llm = AzureChatOpenAI(
            azure_deployment=base_deployment_name,
            api_version="2024-02-15-preview",
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
            azure_endpoint=openai_endpoint,
            api_key=openai_api_key,
        )
    else:
        # Use Azure AD authentication if API key is not provided
        # Use Azure AD authentication if API key is not provided
        default_credential = DefaultAzureCredential()
        # Attempt to get a token to validate authentication
        try:
            default_credential.get_token("https://cognitiveservices.azure.com/.default")
            print("Azure AD authentication successful.")
        except Exception as e:
            raise RuntimeError(f"Azure AD authentication failed: {e}")
        
        llm = AzureChatOpenAI(
            azure_deployment=base_deployment_name,
            api_version="2024-10-21",
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
            azure_endpoint=openai_endpoint,
            azure_ad_token_provider=lambda: default_credential.get_token("https://cognitiveservices.azure.com/.default").token,
        )

    messages = [
    (
        "system",
        system_prompt,
    ),
    ("human", user_input),
]
    try:
        ai_msg = llm.invoke(messages)
        return ai_msg
    except Exception as e:
        print(f"Error invoking LLM: {e}")
        return "failed"

# if __name__ == "__main__":
#     resp = get_llm()
#     print(resp)

