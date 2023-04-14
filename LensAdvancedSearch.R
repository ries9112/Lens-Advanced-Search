library(shiny)
library(bigrquery)
library(DBI)
library(DT)
library(dplyr)
library(stringr)
library(waiter)

bigrquery::bq_auth(path = "mlflow-291816-6d2188fa7f42.json")

ui <- fluidPage(
  titlePanel("Lens Advanced Search"),
  useWaiter(),
  actionButton("open_dialog", "Advanced Search"),
  DTOutput("posts")
)

server <- function(input, output) {
  
  getData <- reactiveVal(NULL)
  
  observeEvent(input$apply_filters, {
    
    bquery_con <- dbConnect(
      bigrquery::bigquery(),
      project = "lens-public-data",
      dataset = "polygon",
      billing = "mlflow-291816"
    )
    
    if (!is.null(input$query)) {
      queryPart <- paste0(" AND LOWER(content) LIKE '%", tolower(input$query), "%'")
    }
    
    if (!is.null(input$dates)) {
      queryPart <- paste0(queryPart, " AND DATE(posts_content.block_timestamp) BETWEEN '", as.character(input$dates[1]), "' AND '", as.character(input$dates[2]), "'")
    }
    
    if (input$user != "") {
      if (str_detect(input$user, ".lens") == FALSE) {
        queryPart <- paste0(queryPart, " AND LOWER(handle) = '", tolower(input$user), ".lens'")
      } else{
        queryPart <- paste0(queryPart, " AND LOWER(handle) = '", tolower(input$user), "'")
      }
    }
    
    if (input$orderdirection == 'descending'){
      order_direction = "DESC"
    } else{
      order_direction = "ASC"
    }
    
    sql_query <- paste0("WITH posts_content AS (
          select
            post_id,
            profile_id,
            content,
            app_id,
            block_timestamp,
            tx_hash
          from `lens-public-data.polygon.public_profile_post`
          order by block_timestamp desc
        ),
        publication_stats AS (
          select
            publication_id,
            total_amount_of_collects,
            total_amount_of_mirrors,
            total_amount_of_comments,
            total_upvotes,
            total_downvotes
          from `lens-public-data.polygon.public_publication_stats`
        ),
        handles AS (
          select 
            profile_id,
            owned_by,
            name,
            handle,
            block_timestamp
          from `lens-public-data.polygon.public_profile`
        )
        
        SELECT 
          name,
          handle,
          posts_content.block_timestamp,
          content,
          app_id,
          total_amount_of_comments,
          total_upvotes, 
          total_amount_of_collects,
          total_amount_of_mirrors,
          total_downvotes,
          posts_content.profile_id,
          publication_id,
          owned_by as address
        FROM posts_content
        LEFT JOIN publication_stats ON publication_stats.publication_id = posts_content.post_id
        LEFT JOIN handles ON handles.profile_id = posts_content.profile_id
        WHERE", queryPart, "
        ORDER BY ",input$orderby ," ", order_direction, "
        LIMIT 10000
        ")
    
    # replace WHERE AND -> WHERE
    sql_query = str_replace(sql_query, "WHERE AND", "WHERE")
    
    waiter_show( # show the waiter
      html = spin_fading_circles() # use a spinner
    )
    
    res <- dbGetQuery(bquery_con, sql_query)
    getData(res)
    
    waiter_hide() # hide the waiter
    
  })
  
  showAdvancedSearch <- function() {
    showModal(modalDialog(
      title = "Advanced Search",
      textInput("query", "Search for posts including text:"),
      dateRangeInput("dates", "Date range:", "2022-05-01"),
      textInput("user", "From user:"),
      selectInput("orderby", "Order results by:", choices = c("block_timestamp","total_upvotes","total_amount_of_comments","total_amount_of_mirrors","total_amount_of_collects"), selected = "total_upvotes"),
      selectInput("orderdirection", "Order results direction:", choices = c("descending", "ascending"), selected = "descending"),
      footer = tagList(
        actionButton("apply_filters", "Search"),
        modalButton("Close Window")
      )
    ))
  }
  
  output$posts <- renderDT({
    req(getData())
    data <- isolate({
      queryResults <- getData()
      queryResults
    })
    # add hyperlinks
    data = mutate(data, lenster_url = paste0('<a href="https://lenster.xyz/posts/', publication_id, '">https://lenster.xyz/posts/', publication_id, '</a>'))
    data = select(data, lenster_url, everything())
    # show table
    datatable(data,
              filter = "top",
              escape=FALSE,
              extensions = "Buttons", rownames = FALSE,
              options = list(paging = FALSE,
                             scrollX=TRUE, 
                             scrollY = "470px",
                             searching = TRUE,
                             ordering = TRUE,
                             dom = 'Bfrtip',
                             buttons = c('copy', 'csv', 'excel', 'pdf'),
                             pageLength=200000, 
                             lengthMenu=c(5,7,10) ))
  })
  
  observeEvent(input$open_dialog, {
    showAdvancedSearch()
  })
  
}

shinyApp(ui, server)
