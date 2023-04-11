with subscription_periods as (
    select subscription_id, 
           customer_id, 
           cast(start_date as date) as start_date, 
           cast(end_date as date) as end_date, 
           monthly_amount 
        from {{subscription_periods}}
),

months as (
    select cast(date_month as date) as date_month from {{util_months}}
),

customers as (
    select
        customer_id,
        date_trunc('month', min(start_date)) as date_month_start,
        date_trunc('month', max(end_date)) as date_month_end
    from subscription_periods
    group by 1
),

customer_months as (
    select
        customers.customer_id,
        months.date_month
    from customers
    inner join months
        on  months.date_month >= customers.date_month_start
        and months.date_month < customers.date_month_end
),

joined as (
    select
        customer_months.date_month,
        customer_months.customer_id,
        coalesce(subscription_periods.monthly_amount, 0) as mrr
    from customer_months
    left join subscription_periods
        on customer_months.customer_id = subscription_periods.customer_id
        and customer_months.date_month >= subscription_periods.start_date
        and (customer_months.date_month < subscription_periods.end_date
                or subscription_periods.end_date is null)
),

customer_revenue_by_month as (
    select
        date_month,
        customer_id,
        mrr,
        mrr > 0 as is_active,
        min(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) as first_active_month,

        max(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) as last_active_month,

        case
          when min(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) = date_month then true
          else false end as is_first_month,
        case
          when max(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) = date_month then true
          else false end as is_last_month
    from joined
),

joined1 as (

    select
        date_month + interval '1 month' as date_month,
        customer_id,
        0::float as mrr,
        false as is_active,
        first_active_month,
        last_active_month,
        false as is_first_month,
        false as is_last_month

    from customer_revenue_by_month

    where is_last_month

)

select * from joined1;
