import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { I18nProvider } from "@react-aria/i18n";

import App from "./App";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <I18nProvider locale="zh-CN">
      <App />
    </I18nProvider>
  </StrictMode>,
);
