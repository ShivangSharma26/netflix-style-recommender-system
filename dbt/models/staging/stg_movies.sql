-- Yeh model raw movies data ko clean karega
with source as (

    select * from {{ source('netflix_raw', 'movies') }}

),

renamed as (

    select
        movieId as movie_id, -- Column names ko standard snake_case mein badal rahe hain
        title,
        genres
    from source

)

select * from renamed