# React Web App

A single-page application to monitor and predict anomalies in buoy motion for Blue Ocean Gear. Bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

## Getting Started Locally

**(1)** Install [Node.js](https://nodejs.org/en/download/) if it is not already on your machine. The download also comes with the Node Package Manager, `npm`, by default.

**(2)** Navigate to the project directory (`dashboard`) and run
```
npm install
```
to install all required packages for the project locally. The resulting `node_modules` folder will not get checked into the code base.

**(3)** If testing locally with the mock [json-server](https://www.npmjs.com/package/json-server), double check that the package is installed globally using the command:
```
npm install -g json-server
```

Then start the json-server with the command:
```
json-server --watch db.json --port 3004 --routes routes.json

---- Windows 10 you can run ----

npx json-server --watch db.json --port 3004 --routes routes.json

```

This tells json-server to pull data from `db.json` and use the special routing rules listed in `routes.json`, both files located under the root of the project directory. The `--port` option allows JSON data to be served from a port of your choice rather than the default port of `3000`. Because `3000` is used by the React app, we use `3004` instead. 

To illustrate how the package works, the `db.json` and `routes.json` file have been configured with sample data. For example, an HTTP GET request to the following URL:

```
http://localhost:3004/api/routeprefix/posts
```

Will return the response payload:
```
[
  {
    "id": 1,
    "title": "json-server",
    "author": "typicode"
  }
]
```

Navigate to one of the exposed API endpoints in a web browser or use an API testing tool like [Postman](https://www.postman.com/) to confirm that data is being served by json-server correctly. Updating and saving `db.json` or `routes.json` will cause the json-server to reload with the new changes. Quit the server anytime using `ctrl + C`.

**(4)** Open a new terminal and navigate to the project directory, `dashboard`. Then run
```
npm start
```

This runs the React app in development mode. Open [http://localhost:3000](http://localhost:3000) to view it in the browser. The page will automatically reload if you make edits. You will also see any lint errors in the console. Quit the server using `ctrl + C`.

## Getting Started with Docker
TODO 


## Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).
