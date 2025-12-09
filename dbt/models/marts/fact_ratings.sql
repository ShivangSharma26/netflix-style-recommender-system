-- Yeh model staging tables ko join karke final table banata hai
with ratings as (

    select * from {{ ref('stg_ratings') }} -- Staging model ko reference kar rahe hain

),

movies as (

    select * from {{ ref('stg_movies') }} -- Staging model ko reference kar rahe hain

),

final as (

    select
        r.user_id,
        r.movie_id,
        m.title as movie_title, -- Movie ka naam bhi le rahe hain
        r.rating,
        r.timestamp
    from ratings r
    left join movies m
        on r.movie_id = m.movie_id -- Dono tables ko movie_id par join kar rahe hain

)

select * from final