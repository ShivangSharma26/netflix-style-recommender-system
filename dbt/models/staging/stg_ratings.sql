-- Yeh model raw ratings data ko clean karega
with source as (

    select * from {{ source('netflix_raw', 'ratings') }}

),

renamed as (

    select
        userId as user_id,
        movieId as movie_id,
        rating,
        timestamp
    from source

)

select * from renamed